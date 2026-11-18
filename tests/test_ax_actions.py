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
        self.journal = SimpleNamespace(path=database)


def test_the_private_record_is_removed_when_nothing_is_in_flight(monkeypatch, tmp_path):
    seen = {}
    monkeypatch.setattr("almanak.gateway.service.GatewayService", Service)
    monkeypatch.setattr(ax_actions.tempfile, "mkdtemp", lambda prefix: str(tmp_path / "record"))
    (tmp_path / "record").mkdir()

    def perform(service, action, amount, *, execute, create, interrupt):
        seen.update(service.options, create=create)
        return {"action": action}

    monkeypatch.setattr(ax_actions, "perform", perform)
    assert ax_actions.run("config", None, "withdraw", None, execute=False) == {"action": "withdraw"}
    assert seen == {"lease_seconds": 30, "operator_actions": True, "create": "config"}
    assert not (tmp_path / "record").exists()

    (tmp_path / "record").mkdir()
    monkeypatch.setattr(ax_actions, "perform", lambda *a, **k: (_ for _ in ()).throw(ValueError))
    with pytest.raises(ValueError):
        ax_actions.run("config", None, "withdraw", None, execute=True)
    assert not (tmp_path / "record").exists()


def test_a_signed_step_in_flight_keeps_its_record(monkeypatch, tmp_path, capsys):
    record = tmp_path / "record"
    record.mkdir()
    (record / "state.sqlite").touch()

    class Pending(Service):
        def __init__(self, *arguments, **options):
            super().__init__(*arguments, **options)
            self.journal.load = lambda: SimpleNamespace(pending=object())

    monkeypatch.setattr("almanak.gateway.service.GatewayService", Pending)
    monkeypatch.setattr(ax_actions.tempfile, "mkdtemp", lambda prefix: str(record))
    monkeypatch.setattr(ax_actions, "perform", lambda *a, **k: (_ for _ in ()).throw(OSError))
    with pytest.raises(OSError):
        ax_actions.run("config", None, "supply", 5, execute=True)
    assert record.exists()
    assert json.loads(capsys.readouterr().out) == {"event": "record_kept", "path": str(record)}


def outcome(kind, success=True):
    return SimpleNamespace(operation=SimpleNamespace(kind=kind), success=success, tx_hash="0x1")


def finished(outcomes, refused=None):
    state = SimpleNamespace(owned=[SimpleNamespace(shares=0, drawn_shares=0)])
    runtime = SimpleNamespace(journal=SimpleNamespace(load=lambda: state), refused=refused)
    return ax_actions.executed_result(runtime, "supply", outcomes)


def test_only_the_actions_own_successful_step_completes_it():
    from almanak.gateway.ethereum import DataError

    assert finished([outcome("approve"), outcome("supply")])["completed"] is True
    reverted = finished([outcome("approve"), outcome("supply", success=False)])
    assert reverted["executed"] and not reverted["completed"]
    # An approval can succeed before the supply is refused: sent, but not completed.
    partial = finished([outcome("approve")], DataError("gas_fee_quote", "above the ceiling"))
    assert partial["executed"] and not partial["completed"]
    assert partial["refused"] == {"kind": "gas_fee_quote", "message": "above the ceiling"}
    # A step refused at signing leaves no outcome at all.
    unsent = finished([], DataError("deadline_margin", "too late"))
    assert (unsent["executed"], unsent["completed"]) == (False, False)
    assert unsent["refused"]["kind"] == "deadline_margin"


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


def test_a_signal_after_the_last_check_does_not_discard_the_result(monkeypatch, tmp_path):
    import os
    import signal

    monkeypatch.setattr("almanak.gateway.service.GatewayService", Service)
    monkeypatch.setattr(ax_actions.tempfile, "mkdtemp", lambda prefix: str(tmp_path / "record"))
    (tmp_path / "record").mkdir()

    def perform(service, action, amount, **options):
        os.kill(os.getpid(), signal.SIGTERM)
        return {"action": action, "completed": True}

    monkeypatch.setattr(ax_actions, "perform", perform)
    result = ax_actions.run("config", None, "withdraw", None, execute=True)
    assert result == {"action": "withdraw", "completed": True}
    assert not (tmp_path / "record").exists()


def test_a_signal_between_steps_stops_the_action_and_reports_its_kept_record(
    monkeypatch, tmp_path, capsys
):
    import os
    import signal

    record = tmp_path / "record"
    record.mkdir()
    (record / "state.sqlite").touch()

    def inspected():
        # A second signal while the record is inspected cannot skip the report either.
        os.kill(os.getpid(), signal.SIGTERM)
        return SimpleNamespace(pending=object())

    class Pending(Service):
        def __init__(self, *arguments, **options):
            super().__init__(*arguments, **options)
            self.journal.load = inspected

    def perform(service, action, amount, *, interrupt, **options):
        os.kill(os.getpid(), signal.SIGTERM)
        interrupt()
        raise AssertionError("the check did not stop the action")

    monkeypatch.setattr("almanak.gateway.service.GatewayService", Pending)
    monkeypatch.setattr(ax_actions.tempfile, "mkdtemp", lambda prefix: str(record))
    monkeypatch.setattr(ax_actions, "perform", perform)
    with pytest.raises(ax_actions.Terminated):
        ax_actions.run("config", None, "supply", 5, execute=True)
    assert json.loads(capsys.readouterr().out) == {"event": "record_kept", "path": str(record)}
    assert record.exists()


def test_execution_checks_for_an_interrupt_between_steps(desk):
    runtime = desk.runtime()
    runtime.submit(desk.intent("supply"))
    calls = []

    def interrupt():
        calls.append(len(desk.kinds()))
        if len(calls) == 3:
            raise ax_actions.Terminated(143)

    with pytest.raises(ax_actions.Terminated):
        runtime.execute(interrupt)
    # Stopped part way: what was recorded before the check stays recorded.
    assert calls and desk.store.load().decision is not None


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
