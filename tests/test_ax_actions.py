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
from almanak.connectors.aave_v4 import direct as aave_direct
from almanak.connectors.aave_v4.lifecycle import adopt_ownership, commit_ownership
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet
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
def named(recorded, monkeypatch):
    """The capture's allocation under the name a direct deployment gives its position."""
    monkeypatch.setattr(aave_direct, "ALLOCATION", recorded.config.allocations[0].name)


@pytest.fixture
def desk(tmp_path, recorded, monkeypatch, named):
    desk = Desk(tmp_path, recorded)

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
    result = ax_actions.perform(desk.service, ActionRequest("supply", amount), execute=False)
    assert result["executed"] is False and "outcomes" not in result
    assert result["first_step"]["operation"]["kind"] == "approve"
    assert result["position_before"] == {"supplied_shares": "0", "debt_shares": "0"}
    assert desk.gateway.broadcasts == [] and desk.store.load().pending is None


def test_execute_runs_the_action_and_reports_the_position(desk):
    amount = desk.intent("supply").amount.amount
    supplied = ax_actions.perform(desk.service, ActionRequest("supply", amount), execute=True)
    assert [item["operation"] for item in supplied["outcomes"]] == ["approve", "supply"]
    assert all(item["success"] and item["transaction"] for item in supplied["outcomes"])
    assert supplied["executed"] and supplied["completed"] and "refused" not in supplied
    shares = desk.gateway.outcome("supply").shares
    assert supplied["position_after"]["supplied_shares"] == str(shares)

    withdrawn = ax_actions.perform(desk.service, ActionRequest("withdraw"), execute=True)
    assert withdrawn["position_before"]["supplied_shares"] == str(shares)
    assert withdrawn["position_after"]["supplied_shares"] == "0"


def test_an_action_states_its_amount_exactly_once(recorded, monkeypatch):
    monkeypatch.setattr(aave_direct, "ALLOCATION", recorded.config.allocations[0].name)

    def intent(action, amount=None):
        return aave_direct.intent(recorded.config, (), ActionRequest(action, amount))

    assert intent("withdraw").kind == "withdraw"
    assert intent("supply", 7).amount.amount == 7
    with pytest.raises(ValueError, match="requires --amount"):
        intent("supply")
    with pytest.raises(ValueError, match="takes no --amount"):
        intent("withdraw", 7)


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
    wallet = Wallet(account="treasury", address="0x" + "ab" * 20)
    positions = aave_direct.positions(
        None, None, SimpleNamespace(reserve=reserve), wallet, ActionRequest("supply", 1)
    )
    config = ax_actions.configuration(
        "aave_v4",
        positions,
        wallet,
        ax_actions.alchemy(reserve.chain.chain_id),
        reserve.chain,
        ax_actions.FeeLimits(max_gas_price_wei=5, max_transaction_fee=9),
    )
    binding = config.accounts.bindings[0]
    # The binding carries the keystore entry's own name, so any wallet's keystore loads.
    assert binding.name == "treasury" and binding.profile.kind == "eoa"
    assert binding.profile.fund.address == "0x" + "ab" * 20
    assert config.target(ALLOCATION).reserve == reserve
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


def test_supply_requires_an_amount_and_fee_ceilings_and_withdraw_may_state_one(capsys):
    supply = parse("supply", *COMMON, "--amount", "5")
    assert (supply.product, supply.amount, supply.execute) == ("operator-action", 5, False)
    withdraw = parse("withdraw", *COMMON, "--execute")
    assert withdraw.amount is None and withdraw.execute
    assert parse("withdraw", *COMMON, "--amount", "5").amount == 5
    for arguments in (
        ("supply", *COMMON),
        ("supply", *COMMON, "--amount", "0"),
        ("withdraw", *COMMON, "--amount", "0"),
        ("withdraw", *COMMON[:-2]),
        ("supply", *COMMON, "--amount", "5", "--config", "market.json"),
    ):
        with pytest.raises(SystemExit):
            parse(*arguments)
    capsys.readouterr()


POOL = [
    *COMMON[:1],
    "aave_v3",
    *COMMON[2:4],
    "--asset",
    "eip155:1/erc20:0x" + "a" * 40,
    *COMMON[8:],
]


def test_each_lending_command_states_its_own_request(capsys):
    borrow = parse("borrow", *POOL, "--amount", "5", "--min-health-factor", "1.5")
    assert (borrow.amount, borrow.min_health_factor) == (5, 15 * 10**17)
    repay = parse("repay", *POOL)
    assert repay.amount is None and not hasattr(repay, "min_health_factor")
    assert parse("repay", *POOL, "--amount", "7").amount == 7
    assert parse("supply", *POOL, "--amount", "5").min_health_factor is None
    assert parse("withdraw", *POOL, "--min-health-factor", "2").min_health_factor == 2 * 10**18
    enabled = parse("collateral", *POOL, "--enable")
    assert (enabled.enable, enabled.amount) == (True, None)
    assert parse("collateral", *POOL, "--disable").enable is False
    for arguments in (
        ("borrow", *POOL, "--amount", "5"),
        ("borrow", *POOL, "--min-health-factor", "1.5"),
        ("borrow", *POOL, "--amount", "5", "--min-health-factor", "1"),
        ("repay", *POOL, "--min-health-factor", "1.5"),
        ("collateral", *POOL),
        ("collateral", *POOL, "--enable", "--disable"),
        ("collateral", *POOL, "--enable", "--amount", "5"),
    ):
        with pytest.raises(SystemExit):
            parse(*arguments)
    capsys.readouterr()


def test_a_request_names_the_intent_it_admits():
    assert ActionRequest("supply", 5).kind == "supply"
    assert ActionRequest("withdraw").kind == "withdraw"
    assert ActionRequest("withdraw", 5).kind == "withdraw_amount"
    assert ActionRequest("repay").kind == "repay"
    assert ActionRequest("repay", 5).kind == "repay_amount"
    assert ActionRequest("borrow", 5).kind == "borrow"
    assert ActionRequest("collateral", enable=False).kind == "collateral"


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
    # These journals are stand-ins with a written checkpoint; the real check is below.
    monkeypatch.setattr(ax_actions, "never_written", lambda database: False)
    monkeypatch.setattr(Stored, "pending", None)
    return Stored


def records(state_home):
    root = state_home / "actions"
    return sorted(path for path in root.iterdir() if path.is_dir()) if root.is_dir() else []


def test_the_private_record_is_removed_when_nothing_is_in_flight(monkeypatch, stored, state_home):
    seen = {}

    def perform(service, request, *, execute, create, interrupt):
        seen.update(service.options, create=create, records=len(records(state_home)))
        return {"action": request.action}

    monkeypatch.setattr(ax_actions, "perform", perform)
    assert ax_actions.run("config", None, ActionRequest("withdraw"), execute=False) == {
        "action": "withdraw"
    }
    assert seen == {"lease_seconds": 30, "operator_actions": True, "create": "config", "records": 1}
    assert records(state_home) == []

    monkeypatch.setattr(ax_actions, "perform", lambda *a, **k: (_ for _ in ()).throw(ValueError))
    with pytest.raises(ValueError):
        ax_actions.run("config", None, ActionRequest("withdraw"), execute=True)
    assert records(state_home) == []


def test_a_service_that_cannot_start_leaves_no_record(monkeypatch, state_home):
    def refuse(*arguments, **options):
        raise ValueError("refused")

    monkeypatch.setattr("almanak.gateway.service.GatewayService", refuse)
    with pytest.raises(ValueError, match="refused"):
        ax_actions.run("config", None, ActionRequest("withdraw"), execute=False)
    assert records(state_home) == []


@pytest.mark.parametrize("pending", [object(), RuntimeError("unreadable")])
def test_a_signed_step_in_flight_keeps_its_record(monkeypatch, stored, state_home, capsys, pending):
    # A record that cannot be read is kept too: it may hold a signed step.
    monkeypatch.setattr(stored, "pending", pending)
    monkeypatch.setattr(ax_actions, "perform", lambda *a, **k: (_ for _ in ()).throw(OSError))
    with pytest.raises(OSError):
        ax_actions.run("config", None, ActionRequest("supply", 5), execute=True)
    (record,) = records(state_home)
    streams = capsys.readouterr()
    # Standard output stays free for the one result document.
    assert streams.out == ""
    assert json.loads(streams.err) == {"event": "record_kept", "path": str(record)}


def test_a_result_returned_with_a_signed_step_pending_keeps_its_record(
    monkeypatch, stored, state_home, capsys
):
    monkeypatch.setattr(stored, "pending", object())
    monkeypatch.setattr(ax_actions, "perform", lambda *a, **k: {"completed": False})
    result = ax_actions.run("config", None, ActionRequest("supply", 5), execute=True)
    (record,) = records(state_home)
    # A returned result names the kept record itself, as one document.
    assert result == {"completed": False, "record_kept": str(record)}
    assert capsys.readouterr() == ("", "")


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
    # What a kill in the middle of cleanup leaves: a record directory without its lock.
    (state_home / "actions/unlocked").mkdir()
    with (running / "lock").open() as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        ax_actions.clear_finished_records()
        # Another command's live record is left alone, with its claim.
        assert running.exists() and running_claim.exists()
    assert not record.exists() and not claimed.exists()
    assert not (state_home / "actions/unlocked").exists()

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


@pytest.mark.usefixtures("named")
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


@pytest.mark.usefixtures("named")
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
    result = ax_actions.perform(desk.service, ActionRequest("supply", amount), execute=True)
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
        ax_actions.perform(desk.service, ActionRequest("supply", amount), execute=True)
    event = json.loads(capsys.readouterr().err)
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
    monkeypatch.setattr("almanak.connectors.direct.require_direct", lambda *a: None)
    monkeypatch.setattr(ax_actions, "read_wallet", lambda directory: {})
    monkeypatch.setattr(ax_actions, "transport", lambda args, chain: (None, "endpoint"))
    monkeypatch.setattr(ax_actions, "action_config", lambda *a: "config")
    backend = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(ax_actions, "production_backend", lambda *a: backend)
    result = {"executed": execute} if completed is None else {"completed": completed}
    monkeypatch.setattr(ax_actions, "run", lambda *a, **k: result)
    args = SimpleNamespace(
        wallet=None,
        env_file=None,
        connector="aave_v4",
        ax_command="supply",
        amount=1,
        execute=execute,
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

    def perform(service, request, **options):
        os.kill(os.getpid(), signal.SIGTERM)
        return {"action": request.action, "completed": True}

    monkeypatch.setattr(ax_actions, "perform", perform)
    result = ax_actions.run("config", None, ActionRequest("withdraw"), execute=True)
    assert result == {"action": "withdraw", "completed": True}
    assert records(state_home) == []


def test_a_signal_between_steps_stops_the_action_and_reports_its_kept_record(
    monkeypatch, stored, state_home, capsys
):
    import os
    import signal

    def perform(service, request, *, interrupt, **options):
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
        ax_actions.run("config", None, ActionRequest("supply", 5), execute=True)
    (record,) = records(state_home)
    streams = capsys.readouterr()
    # Standard output stays free for the one result document.
    assert streams.out == ""
    assert json.loads(streams.err) == {"event": "record_kept", "path": str(record)}


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


@pytest.mark.usefixtures("named")
def test_a_signed_step_without_an_outcome_is_reported_as_unresolved(recorded, capsys):
    (approve,) = outcomes_of(recorded, "approve")
    pending = SimpleNamespace(operation=SimpleNamespace(kind="supply"), tx_hash="0xabc")
    state = SimpleNamespace(
        config=recorded.config, owned=recorded.owned, outcomes=(approve,), pending=pending
    )
    expected = {"operation": "supply", "transaction": "0xabc"}
    result = ax_actions.executed_result(state, [approve], None)
    # It is not an outcome: nothing says yet whether it was mined.
    assert result["unresolved"] == expected and len(result["outcomes"]) == 1
    # A signed step counts as executed even before any outcome exists.
    assert ax_actions.executed_result(state, [], None)["executed"] is True
    assert "unresolved" not in finished(recorded, [approve])

    runtime = SimpleNamespace(journal=SimpleNamespace(load=lambda: state))
    ax_actions.report_sent(runtime, 1)
    assert json.loads(capsys.readouterr().err) == {
        "event": "steps_sent",
        "outcomes": [],
        "unresolved": expected,
    }


def test_the_whole_path_to_a_record_is_synced_on_every_action(state_home, monkeypatch):
    synced = []
    monkeypatch.setattr(ax_actions, "sync_directory", synced.append)
    # An earlier command created an ancestor and stopped before it synced anything.
    state_home.mkdir()
    ax_actions.new_record()[1].close()
    root = state_home / "actions"
    path = [state_home, *state_home.parents[:3], root]
    # Every entry up to the anchor, also for directories that already existed, then
    # the record's own entry.
    assert synced == path
    synced.clear()
    ax_actions.new_record()[1].close()
    assert synced == path


def transient_once(runtime, step):
    """Fail the named runtime step once with a passing provider error."""
    from almanak.gateway.ethereum import DataError

    real, failed = getattr(runtime, step), []

    def failing(*arguments):
        if not failed:
            failed.append(True)
            raise DataError("rate_limit", "provider is rate limited", transient=True)
        return real(*arguments)

    setattr(runtime, step, failing)
    return failed


def test_a_passing_failure_while_a_signed_step_waits_is_asked_again(desk):
    runtime = desk.runtime()
    runtime.submit(desk.intent("supply"))
    failed = transient_once(runtime, "resolve_pending")
    outcomes = runtime.execute()
    # One failed receipt read did not abandon the sent approval.
    assert failed and [item.operation.kind for item in outcomes] == ["approve", "supply"]


def test_a_passing_failure_with_nothing_signed_ends_the_action(desk):
    from almanak.gateway.ethereum import DataError

    runtime = desk.runtime()
    runtime.submit(desk.intent("supply"))
    transient_once(runtime, "prepare_decision")
    with pytest.raises(DataError, match="rate limited"):
        runtime.execute()
    assert desk.gateway.broadcasts == [] and desk.store.load().pending is None


def test_an_outcome_reports_a_wallet_movement_it_does_not_explain(recorded):
    (supply,) = outcomes_of(recorded, "supply")
    assert "accounting_issues" not in ax_actions.outcome_summary(supply)
    flagged = supply.model_copy(update={"accounting_issues": ("unexplained transfer",)})
    assert ax_actions.outcome_summary(flagged)["accounting_issues"] == ["unexplained transfer"]


def test_the_in_process_gateway_takes_renews_and_releases_the_services_lease(tmp_path, recorded):
    """The real service's lease, through the client the direct action uses."""
    desk = Desk(tmp_path, recorded)
    gateway = ax_actions.LocalGateway(desk.service)
    gateway.acquire()
    assert gateway.heartbeat.is_alive() and gateway.lease_error is None
    gateway.scoped("renew")
    assert gateway.lease_confirmed is not None
    # One coordinator per deployment: a second client is refused while the first lives.
    other = ax_actions.LocalGateway(desk.service)
    with pytest.raises(Exception, match="lease|coordinator"):
        other.acquire()
    other.close()
    gateway.close()
    assert not gateway.heartbeat.is_alive() and gateway.coordinator is None
    # The release is real: the next coordinator is admitted at once.
    other = ax_actions.LocalGateway(desk.service)
    other.acquire()
    other.close()


def test_a_journal_without_a_checkpoint_holds_no_signed_step(tmp_path, recorded):
    """A command killed while it created its journal signed nothing."""
    import sqlite3

    (tmp_path / "empty").mkdir()
    (tmp_path / "empty/state.sqlite").touch()
    assert ax_actions.in_flight(tmp_path / "empty") is False
    (tmp_path / "tabled").mkdir()
    with sqlite3.connect(tmp_path / "tabled/state.sqlite") as connection:
        connection.execute("CREATE TABLE checkpoint (id INTEGER PRIMARY KEY, body TEXT)")
    assert ax_actions.in_flight(tmp_path / "tabled") is False
    # A written checkpoint is read for its signed step, and an unreadable file is kept.
    desk = Desk(tmp_path / "written", recorded)
    assert ax_actions.in_flight(desk.store.path.parent) is False
    (tmp_path / "corrupt").mkdir()
    (tmp_path / "corrupt/state.sqlite").write_bytes(b"not a database" * 100)
    assert ax_actions.in_flight(tmp_path / "corrupt") is True


def signed_and_stopped(desk):
    """A supply whose approval was signed and sent, then the command stopped."""
    runtime = desk.runtime()
    runtime.submit(desk.intent("supply"))
    while desk.store.load().pending is None:
        runtime.tick()
    return desk.store.load().pending


def test_recovery_resolves_the_signed_step_and_drops_the_unsigned_rest(desk):
    pending = signed_and_stopped(desk)
    sent = len(desk.gateway.broadcasts)
    result = ax_actions.resolve(desk.service, lambda: None)
    assert result["resolved"] and result["discarded_request"] and "unresolved" not in result
    assert [item["operation"] for item in result["outcomes"]] == ["approve"]
    assert result["outcomes"][0]["transaction"] == pending.tx_hash
    state = desk.store.load()
    # Nothing new was planned or signed: the supply itself is the operator's to restate.
    assert state.pending is None and state.decision is None and state.plan is None
    assert desk.kinds() == ["approve"] and len(desk.gateway.broadcasts) <= sent + 1


def test_recovery_that_cannot_resolve_reports_the_step_and_keeps_it(desk, monkeypatch):
    from almanak.gateway.ethereum import DataError

    pending = signed_and_stopped(desk)
    build = desk.runtime

    def refusing():
        runtime = build()

        def refused(interrupt):
            raise DataError("signed_attempt_unsendable", "current validation refuses it")

        runtime.recover = refused
        return runtime

    monkeypatch.setattr(desk, "runtime", refusing)
    result = ax_actions.resolve(desk.service, lambda: None)
    assert result["resolved"] is False and result["discarded_request"] is False
    assert result["unresolved"]["transaction"] == pending.tx_hash
    assert result["refused"]["kind"] == "signed_attempt_unsendable"
    assert desk.store.load().pending is not None


def test_recover_refuses_what_is_not_a_free_action_record(stored, state_home, tmp_path):
    import fcntl

    with pytest.raises(ValueError, match="not an action record"):
        ax_actions.recover(tmp_path, lambda config: None)
    record, _ = abandoned(state_home, "kept")
    with (record / "lock").open() as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        with pytest.raises(ValueError, match="still using this record"):
            ax_actions.recover(record, lambda config: None)


def test_recover_runs_the_records_own_configuration_and_closes_its_backend(
    monkeypatch, stored, state_home
):
    record, claimed = abandoned(state_home, "kept")
    monkeypatch.setattr(stored, "load", lambda self: SimpleNamespace(config="stored", pending=None))
    closed = []
    backend = SimpleNamespace(close=lambda: closed.append(True))
    seen = {}

    def resolve(service, interrupt):
        seen.update(service.options)
        return {"resolved": True}

    monkeypatch.setattr(ax_actions, "resolve", resolve)

    def backend_for(config):
        seen["config"] = config
        return backend

    assert ax_actions.recover(record, backend_for) == {"resolved": True}
    assert seen == {"config": "stored", "lease_seconds": 30, "operator_actions": True}
    # Resolved, so the record and the wallet's claim are gone.
    assert closed == [True] and not record.exists() and not claimed.exists()


def test_a_record_on_a_delisted_endpoint_stays_kept_until_a_transport_is_named(
    monkeypatch, stored, state_home, capsys
):
    """Removing a pair from the reviewed list must not strand the wallet's claim."""
    from almanak.caip import ChainId
    from almanak.gateway import providers, read_endpoints
    from almanak.models import Chain

    avalanche = ChainId("eip155:43114")
    assert avalanche not in providers.PUBLIC_RPC["drpc"]
    network = SimpleNamespace(
        provider=providers.PublicProvider(kind="public_rpc", name="drpc"),
        chain=Chain(chain_id=avalanche, environment="mainnet"),
    )
    config = SimpleNamespace(production=SimpleNamespace(networks=[network]))
    monkeypatch.setattr(stored, "load", lambda self: SimpleNamespace(config=config, pending=None))
    record, claimed = abandoned(state_home, "kept")
    args = SimpleNamespace(record=record, env_file=None, public_rpc=None)
    with pytest.raises(ValueError, match="no longer serves eip155:43114; name a transport"):
        ax_actions.recover_command(args)
    # Refused before anything ran: the record, its claim and its lock are as they were.
    assert record.exists() and claimed.exists()

    proved, opened = [], []

    class Reader:
        def __init__(self, chain, endpoint):
            proved.append(endpoint)

        def __enter__(self):
            return self

        def __exit__(self, *arguments):
            pass

        def verify_chain(self):
            proved.append("chain")

    def backend(config, endpoint, wallet):
        opened.append((endpoint, wallet))
        return SimpleNamespace(close=lambda: None)

    monkeypatch.setattr("almanak.gateway.ethereum.EthereumReader", Reader)
    monkeypatch.setattr(read_endpoints, "verify_public", lambda reader: proved.append("public"))
    monkeypatch.setattr(ax_actions, "production_backend", backend)
    monkeypatch.setattr(ax_actions, "resolve", lambda service, interrupt: {"resolved": True})
    args.public_rpc = "publicnode"
    assert ax_actions.recover_command(args) == 0
    named = providers.PUBLIC_RPC["publicnode"][avalanche]
    assert proved == [named, "chain", "public"] and opened == [(named, None)]
    assert json.loads(capsys.readouterr().out) == {"resolved": True}
    assert not record.exists() and not claimed.exists()


def test_recover_lists_kept_records_and_needs_credentials_only_to_resolve(
    monkeypatch, stored, state_home, capsys
):
    record, _ = abandoned(state_home, "kept")
    fund = SimpleNamespace(chain=SimpleNamespace(chain_id="eip155:1"), address="0xabc")
    binding = SimpleNamespace(profile=SimpleNamespace(fund=fund))
    config = SimpleNamespace(accounts=SimpleNamespace(bindings=[binding]))
    pending = SimpleNamespace(operation=SimpleNamespace(kind="approve"), tx_hash="0x1")
    state = SimpleNamespace(config=config, pending=pending)
    monkeypatch.setattr(stored, "load", lambda self: state)
    listing = SimpleNamespace(record=None, env_file=None, public_rpc=None)
    assert ax_actions.recover_command(listing) == 0
    assert json.loads(capsys.readouterr().out) == {
        "records": [
            {
                "path": str(record),
                "readable": True,
                "chain": "eip155:1",
                "account": "0xabc",
                "unresolved": {"operation": "approve", "transaction": "0x1"},
            }
        ]
    }
    monkeypatch.setattr(ax_actions, "recover", lambda record, backend_for: {"resolved": False})
    args = SimpleNamespace(record=record, env_file=Path(".env"), public_rpc=None)
    assert ax_actions.recover_command(args) == 1


def test_recover_parses_with_and_without_a_record():
    listing = parse("recover")
    assert (listing.product, listing.record) == ("operator-recover", None)
    resolving = parse("recover", "--record", "kept", "--env-file", ".env")
    assert resolving.record == Path("kept") and resolving.env_file == Path(".env")
