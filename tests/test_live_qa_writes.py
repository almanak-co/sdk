"""The costly write check refuses to start unless every gate holds.

Offline and free: the command runner is a fake, so nothing here contacts an
endpoint, opens a keystore or signs. No test in the repository runs the write
check for real.
"""

import json
from pathlib import Path

import pytest

from almanak.caip import ChainId
from almanak.cli_arguments import build_parser, parse_validated
from scripts.live_qa import __main__ as entry
from scripts.live_qa import cli, writes

SPEND = {writes.SPEND_ENV: "1"}
POSITION_EMPTY = {"supplied_shares": "0", "debt_shares": "0"}
POSITION_HELD = {"supplied_shares": "99", "debt_shares": "0"}
PHRASE = "supply 100000 ethereum-usdc and withdraw"


@pytest.fixture
def wallet(tmp_path):
    """A wallet directory as the gates see it: wallet.json and the password file it names.

    The gates read the path from wallet.json and check the file exists; nothing opens it.
    """
    directory = tmp_path / "wallet"
    directory.mkdir()
    password = directory / "keystore-password"
    password.write_text("not a real password")
    (directory / "wallet.json").write_text(json.dumps({"password_file": str(password)}))
    return directory


def request_for(wallet, **changes):
    fields = {
        "market": "ethereum-usdc",
        "amount": 100_000,
        "wallet": wallet,
        "max_gas_price_wei": 20 * writes.GWEI,
        "max_transaction_fee": 10**16,
        "public_rpc": "publicnode",
    }
    return writes.WriteRequest(**{**fields, **changes})


def never(arguments, timeout=None):
    pytest.fail(f"a refused write check ran a command: {arguments}")


def outcome(operation):
    return {"operation": operation, "success": True, "transaction": "0x" + operation.encode().hex()}


def preview(position=POSITION_EMPTY):
    return {"action": "x", "executed": False, "position_before": position, "first_step": {}}


def executed(operations, position, completed=True):
    document = preview()
    document.update(
        executed=True,
        completed=completed,
        outcomes=[outcome(operation) for operation in operations],
        position_after=position,
    )
    return document


class FakeAlmanak:
    """Answers `ax supply` and `ax withdraw` from a script, recording every command."""

    def __init__(self, **answers):
        self.answers = answers
        self.commands = []
        self.timeouts = []

    def __call__(self, arguments, timeout=cli.READ_TIMEOUT_SECONDS):
        self.commands.append(tuple(arguments))
        self.timeouts.append(timeout)
        key = arguments[1] + ("_execute" if "--execute" in arguments else "_preview")
        answer = self.answers.get(key)
        if answer is None:
            stderr = json.dumps({"error": "refused", "message": "refused during planning"})
            return cli.Invocation(tuple(arguments), 1, "", stderr)
        exit_code = 0 if answer.get("completed", True) else 1
        return cli.Invocation(tuple(arguments), exit_code, json.dumps(answer), "")

    @property
    def sent(self):
        return [command[1] for command in self.commands if "--execute" in command]


def round_trip(**changes):
    answers = {
        "supply_preview": preview(),
        "supply_execute": executed(["approve", "supply"], POSITION_HELD),
        "withdraw_preview": preview(POSITION_HELD),
        "withdraw_execute": executed(["withdraw"], POSITION_EMPTY),
    }
    return FakeAlmanak(**{**answers, **changes})


def check(request, environment, run, tmp_path, ask=None):
    said = []
    # A new directory for every run, as the check itself requires.
    output = tmp_path / f"report-{len(list(tmp_path.iterdir()))}"
    code = writes.run_writes(request, environment, run, output, ask=ask, say=said.append)
    return code, "\n".join(said), output


# The gates


def test_it_refuses_without_the_spend_flag(wallet, tmp_path):
    for environment in ({}, {writes.SPEND_ENV: "0"}, {writes.SPEND_ENV: "true"}):
        code, said, output = check(request_for(wallet), environment, never, tmp_path)
        assert code == 2 and "ALMANAK_QA_SPEND_REAL_FUNDS=1 is required" in said
        assert not output.exists()


def test_it_refuses_without_a_wallet_the_operator_supplied(wallet, tmp_path):
    code, said, _ = check(request_for(None), SPEND, never, tmp_path)
    assert code == 2 and "--wallet is required" in said
    code, said, _ = check(request_for(tmp_path / "missing"), SPEND, never, tmp_path)
    assert code == 2 and "holds no wallet.json" in said


def test_without_a_terminal_it_refuses_a_wallet_that_would_ask_for_its_password(wallet, tmp_path):
    """`almanak ax` would ask for the password after the confirmation, with nobody to answer."""
    named = json.loads((wallet / "wallet.json").read_text())["password_file"]
    for record in ("{}", "not json", "[]", json.dumps({"password_file": named + ".gone"})):
        (wallet / "wallet.json").write_text(record)
        code, said, _ = check(request_for(wallet), SPEND, never, tmp_path)
        assert code == 2 and "must name an existing password file" in said, record
    # At a terminal the person can type it, so the same wallet is accepted.
    (wallet / "wallet.json").write_text("{}")
    almanak = round_trip()
    code, _, _ = check(request_for(wallet), SPEND, almanak, tmp_path, ask=lambda prompt: "")
    assert code == 0 and len(almanak.commands) == 1


def test_the_gates_never_open_the_password_file_or_a_keystore(wallet, monkeypatch):
    opened = []
    real_open = Path.open

    def recording(path, *args, **kwargs):
        opened.append(path.name)
        return real_open(path, *args, **kwargs)

    (wallet / "keystores.json").write_text("{}")
    monkeypatch.setattr(Path, "open", recording)
    assert writes.gates(request_for(wallet), SPEND, interactive=False) == []
    assert opened == ["wallet.json"]


def test_it_refuses_an_output_directory_that_already_holds_a_report(wallet, tmp_path):
    """An earlier report may hold the only record of the transactions a run sent."""
    earlier = tmp_path / "earlier"
    earlier.mkdir()
    (earlier / "summary.md").write_text("transactions")
    said = []
    code = writes.run_writes(request_for(wallet), SPEND, never, earlier, say=said.append)
    assert code == 2 and "is not empty; use a new output directory" in said[0]
    assert (earlier / "summary.md").read_text() == "transactions"
    assert writes.output_gates(tmp_path / "missing") == []


def test_it_refuses_an_output_it_could_not_write_a_report_into(wallet, tmp_path):
    """Found before the first command, not after the transactions were sent."""
    said = []
    regular = tmp_path / "report.md"
    regular.write_text("")
    code = writes.run_writes(request_for(wallet), SPEND, never, regular, say=said.append)
    assert code == 2 and "is not a directory" in said[0] and regular.read_text() == ""
    # A path that passes the gates but cannot be made is refused as well.
    beneath = regular / "nested"
    code = writes.run_writes(request_for(wallet), SPEND, never, beneath, say=said.append)
    assert code == 2 and "cannot be created" in said[-1]


def test_the_report_directory_exists_before_the_first_command(wallet, tmp_path):
    output = tmp_path / "new" / "report"
    seen = []

    def run(arguments, timeout=None):
        seen.append(output.is_dir())
        return round_trip()(arguments, timeout)

    assert writes.run_writes(request_for(wallet), SPEND, run, output, say=lambda text: None) == 0
    assert seen == [True]


@pytest.mark.parametrize(
    ("field", "option"),
    [
        ("max_gas_price_wei", "--max-gas-price-wei"),
        ("max_transaction_fee", "--max-transaction-fee"),
    ],
)
def test_it_refuses_without_each_fee_ceiling(wallet, tmp_path, field, option):
    code, said, _ = check(request_for(wallet, **{field: None}), SPEND, never, tmp_path)
    assert code == 2 and f"{option} is required" in said
    for value in (0, 10**30):
        code, said, _ = check(request_for(wallet, **{field: value}), SPEND, never, tmp_path)
        assert code == 2 and f"{option} must be above 0 and at most" in said


@pytest.mark.parametrize("name", writes.CI_ENV)
@pytest.mark.parametrize("value", ["true", "1", "false", ""])
def test_it_refuses_in_ci_whatever_else_is_set(wallet, tmp_path, name, value):
    """Set to anything, an empty value included: the gate fails closed."""
    code, said, _ = check(request_for(wallet), {**SPEND, name: value}, never, tmp_path)
    assert code == 2 and f"{name} is set: the write check never runs in CI" in said


def test_it_refuses_anything_but_a_tiny_amount_in_a_reviewed_market(wallet, tmp_path):
    for changes, reason in (
        ({"market": None}, "--market is required"),
        ({"market": "ethereum-dai"}, "--market is required"),
        ({"amount": None}, "--amount is required in base units; suggested 100000"),
        ({"amount": 0}, "tiny amounts only"),
        ({"amount": 1_000_001}, "at most 1000000 base units of USDC"),
    ):
        code, said, _ = check(request_for(wallet, **changes), SPEND, never, tmp_path)
        assert code == 2 and reason in said, changes


def test_it_refuses_without_exactly_one_named_transport(wallet, tmp_path):
    for changes in ({"public_rpc": None}, {"env_file": Path(".env")}):
        code, said, _ = check(request_for(wallet, **changes), SPEND, never, tmp_path)
        assert code == 2 and "exactly one of --public-rpc and --env-file" in said


def test_every_unmet_gate_is_named_at_once_and_none_touches_the_network(tmp_path):
    refusals = writes.gates(writes.WriteRequest(), {"CI": "true"})
    assert len(refusals) == 7
    code, said, output = check(writes.WriteRequest(execute=True), {}, never, tmp_path)
    assert code == 2 and said.startswith("The write check refuses to start:")
    assert not output.exists()


def test_the_entry_point_refuses_before_running_anything(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv(writes.SPEND_ENV, raising=False)
    monkeypatch.setattr(cli, "runner", lambda command, cwd: never)
    output = tmp_path / "report"
    code = entry.main(["writes", "--output", str(output), "--execute"])
    assert code == 2 and not output.exists()
    stderr = capsys.readouterr().err
    for reason in ("ALMANAK_QA_SPEND_REAL_FUNDS=1", "--wallet", "--max-gas-price-wei"):
        assert reason in stderr


def test_a_market_carries_a_validated_chain_and_its_fee_limit_is_found_by_it():
    for market in writes.MARKETS.values():
        assert isinstance(market.chain, ChainId) and market.chain in writes.FEE_LIMITS
    assert all(isinstance(chain, ChainId) for chain in writes.FEE_LIMITS)
    with pytest.raises(ValueError):
        ChainId("ethereum")


def test_the_limits_keep_every_market_tiny():
    for name, market in writes.MARKETS.items():
        assert 0 < market.suggested <= market.maximum, name
        # At most one whole stablecoin, or a thousandth of a wrapped ETH, a tenth of an AVAX.
        whole = market.maximum / 10**market.decimals
        assert whole <= {"USDC": 1, "WETH": 0.001, "WAVAX": 0.1}[market.symbol], name
        assert market.chain in writes.FEE_LIMITS


# The flow, once every gate holds


def test_without_execute_it_only_previews_and_signs_nothing(wallet, tmp_path):
    almanak = round_trip()
    code, said, output = check(request_for(wallet), SPEND, almanak, tmp_path)
    assert code == 0 and almanak.sent == [] and len(almanak.commands) == 1
    assert "THIS SPENDS REAL FUNDS ON MAINNET" in said
    assert f'--execute --confirm "{PHRASE}"' in said and "nothing was signed" in said
    assert json.loads((output / "writes.json").read_text())["verdict"] == "preview only"


def test_execute_without_the_confirmation_signs_nothing(wallet, tmp_path):
    for changes, ask in (({}, None), ({"confirm": "yes"}, None), ({}, lambda prompt: "y")):
        almanak = round_trip()
        request = request_for(wallet, execute=True, **changes)
        code, said, _ = check(request, SPEND, almanak, tmp_path, ask=ask)
        assert code == 2 and almanak.sent == [] and "Not confirmed" in said


def test_a_confirmed_round_trip_previews_supplies_and_withdraws_in_order(wallet, tmp_path):
    almanak = round_trip()
    request = request_for(wallet, execute=True, confirm=PHRASE)
    code, said, output = check(request, SPEND, almanak, tmp_path)
    assert code == 0 and "round trip completed" in said
    steps = [(command[1], "--execute" in command) for command in almanak.commands]
    assert steps == [("supply", False), ("supply", True), ("withdraw", False), ("withdraw", True)]
    # A preview is bounded; an executing command is never timed out, so never killed.
    assert almanak.timeouts == [cli.READ_TIMEOUT_SECONDS, None, cli.READ_TIMEOUT_SECONDS, None]
    summary = (output / "summary.md").read_text()
    for operation in ("approve", "supply", "withdraw"):
        assert f"{operation} 0x{operation.encode().hex()} (succeeded)" in summary


def test_a_typed_confirmation_at_a_terminal_is_accepted(wallet, tmp_path):
    prompts = []

    def ask(prompt):
        prompts.append(prompt)
        return PHRASE

    almanak = round_trip()
    code, _, _ = check(request_for(wallet, execute=True), SPEND, almanak, tmp_path, ask=ask)
    assert code == 0 and almanak.sent == ["supply", "withdraw"] and PHRASE in prompts[0]


def test_a_wallet_that_already_holds_a_position_is_refused(wallet, tmp_path):
    almanak = round_trip(supply_preview=preview(POSITION_HELD))
    request = request_for(wallet, execute=True, confirm=PHRASE)
    code, said, _ = check(request, SPEND, almanak, tmp_path)
    assert code == 1 and almanak.sent == [] and "already holds a position" in said


def test_a_position_that_appeared_after_the_preview_is_never_withdrawn(wallet, tmp_path):
    """The supply adopts what the wallet holds when it runs; the withdrawal takes it all."""
    adopted = executed(["approve", "supply"], POSITION_HELD)
    adopted["position_before"] = POSITION_HELD
    almanak = round_trip(supply_execute=adopted)
    request = request_for(wallet, execute=True, confirm=PHRASE)
    code, said, _ = check(request, SPEND, almanak, tmp_path)
    assert code == 1 and almanak.sent == ["supply"] and len(almanak.commands) == 2
    assert "position changed between the preview and the supply" in said
    assert "THE SUPPLIED AMOUNT IS STILL IN THE MARKET" in said


def test_a_refused_preview_stops_before_the_confirmation(wallet, tmp_path):
    almanak = round_trip(supply_preview=None)
    request = request_for(wallet, execute=True, confirm=PHRASE)
    code, said, _ = check(request, SPEND, almanak, tmp_path, ask=pytest.fail)
    assert code == 1 and almanak.sent == [] and "preview was refused" in said


def test_a_supply_that_did_not_complete_is_never_followed_by_anything(wallet, tmp_path):
    partial = executed(["approve"], POSITION_EMPTY, completed=False)
    almanak = round_trip(supply_execute=partial)
    request = request_for(wallet, execute=True, confirm=PHRASE)
    code, said, output = check(request, SPEND, almanak, tmp_path)
    assert code == 1 and almanak.sent == ["supply"] and len(almanak.commands) == 2
    assert "Do not run this again" in said and "almanak ax recover" in said
    assert "approve" in (output / "summary.md").read_text()


def test_a_failed_withdrawal_says_the_funds_may_still_be_supplied(wallet, tmp_path):
    request = request_for(wallet, execute=True, confirm=PHRASE)
    for changes, reason in (
        ({"withdraw_preview": None}, "THE SUPPLIED AMOUNT IS STILL IN THE MARKET"),
        (
            {"withdraw_execute": executed([], POSITION_HELD, completed=False)},
            "Do not withdraw again yet",
        ),
        (
            {"withdraw_execute": executed(["withdraw"], POSITION_HELD)},
            "the position is not empty",
        ),
    ):
        code, said, _ = check(request, SPEND, round_trip(**changes), tmp_path)
        assert code == 1 and reason in said, changes


# The commands are the real CLI's


@pytest.mark.parametrize("market", sorted(writes.MARKETS))
@pytest.mark.parametrize("transport", [{"public_rpc": "drpc"}, {"env_file": Path(".env")}])
def test_each_action_is_a_command_almanak_ax_accepts(wallet, market, transport):
    request = request_for(wallet, market=market, **{"public_rpc": None, **transport})
    for name in ("supply", "withdraw"):
        for execute in (False, True):
            arguments = writes.action(request, name, execute=execute)
            args = parse_validated(build_parser(), list(arguments))
            assert args.execute is execute and args.wallet == wallet
            assert args.max_gas_price_wei and args.max_transaction_fee
            assert (args.amount == 100_000) if name == "supply" else ("--amount" not in arguments)


def test_the_help_carries_the_warning_and_requires_nothing_by_itself(capsys):
    with pytest.raises(SystemExit) as stopped:
        entry.main(["writes", "--help"])
    assert stopped.value.code == 0
    assert "THIS SPENDS REAL FUNDS ON MAINNET" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        entry.main(["writes", "--output", "x", "--amount", "0.1"])
    assert writes.terminal_prompt() in (None, input)
