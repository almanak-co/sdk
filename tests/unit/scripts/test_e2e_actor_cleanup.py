import sqlite3
from types import SimpleNamespace

import pytest

from almanak.framework.anvil.accounts import anvil_default_address
from almanak.framework.runner.identity import resolve_deployment_id
from qa_lab import e2e_actor_cleanup as actor_cleanup
from qa_lab.e2e_card import canonical, digest, load_json
from tests.unit.scripts import test_e2e_actor_terminal as terminal_tests
from tests.unit.scripts import test_e2e_cleanup as subject_tests

run = subject_tests.run
fork = terminal_tests.fork


@pytest.fixture
def lane(run, monkeypatch):
    root = run.context.root
    actor = root / "actor"
    actor.mkdir()
    config = {
        "subject_wallet": run.wallet,
        "fork_instance": "unit-fork",
        "fork_block": 1,
        "fork_hash": "0x" + "11" * 32,
    }
    (actor / "config.json").write_bytes(canonical(config))
    resources = {}
    for name in ("strategy.py", "__init__.py"):
        raw = b"# synthetic actor source for dispatch tests\n"
        (actor / name).write_bytes(raw)
        resources[f"qa_lab/strategies/lp_stimulus/{name}"] = digest(raw)
    (root / "preparation/resources.json").write_bytes(canonical(resources))
    (actor / "binding.json").write_bytes(
        canonical(
            {
                "bindings": {
                    "preparation_sha256": digest((root / "preparation/card.json").read_bytes()),
                    "config_sha256": digest((actor / "config.json").read_bytes()),
                }
            }
        )
    )
    identity = resolve_deployment_id(wallet_address=anvil_default_address(1), chain="arbitrum")
    with sqlite3.connect(actor / "almanak_state.db") as db:
        db.execute("CREATE TABLE strategy_state (deployment_id TEXT)")
        db.execute("INSERT INTO strategy_state VALUES (?)", (identity,))
        db.execute(
            "CREATE TABLE teardown_requests (deployment_id TEXT,status TEXT,positions_failed INTEGER,requested_at TEXT,acknowledged_at TEXT,started_at TEXT,completed_at TEXT)"
        )
    token = run.store.reserve_launch(run.lease, "stimulus")
    run.store.claim_launch(run.lease, role="stimulus", token=token)
    subject_tests.complete(run)
    run.context.instance_id, run.context.fork_hash = config["fork_instance"], config["fork_hash"]
    monkeypatch.setattr(actor_cleanup, "capture_position_generations", lambda *args: run.terminal)
    monkeypatch.setattr(
        actor_cleanup, "observe_actor_terminal", lambda *args: {"status": "PASS", "scope": "stubbed_inventory"}
    )
    monkeypatch.setattr(
        actor_cleanup,
        "capture_stimulus_quantities",
        lambda *args, **kwargs: {"status": "PASS", "scope": "stubbed_quantities"},
    )
    commands = []

    def signal(argv, **kwargs):
        commands.append(argv)
        with sqlite3.connect(actor / "almanak_state.db") as db:
            db.execute(
                "INSERT INTO teardown_requests VALUES (?, 'completed', 0, ?, ?, ?, ?)",
                (identity, *[f"2026-09-07T00:00:0{i}+00:00" for i in range(4)]),
            )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subject_tests.subprocess, "run", signal)
    return run, actor, commands


def execute(lane, name="actor-cleanup"):
    run, _, _ = lane
    return actor_cleanup.cleanup_stimulus(
        run.context, run.store, run.lease, output=run.context.root / name, timeout_seconds=1
    )


def test_stimulus_signal_follows_raw_subject_closure_and_is_not_reissued(lane):
    run, actor, commands = lane
    assert execute(lane)["status"] == "PASS"
    assert len(commands) == 1
    assert commands[0][commands[0].index("-d") + 1] == str(actor)
    assert commands[0][commands[0].index("-s") + 1] == resolve_deployment_id(
        wallet_address=anvil_default_address(1), chain="arbitrum"
    )
    assert load_json(run.context.root / "actor-cleanup/subject-before-unwind.json") == run.terminal
    assert execute(lane, "actor-adoption")["status"] == "PASS"
    assert len(commands) == 1


@pytest.mark.parametrize("fault", ["subject_open", "subject_incomplete", "source", "config", "fork", "lease"])
def test_unsafe_actor_unwind_is_refused_before_dispatch(lane, monkeypatch, fault):
    run, actor, commands = lane
    if fault == "subject_open":
        monkeypatch.setattr(actor_cleanup, "capture_position_generations", lambda *args: run.opened)
    elif fault == "subject_incomplete":
        with sqlite3.connect(run.database) as db:
            db.execute("UPDATE teardown_requests SET status='executing'")
    elif fault == "source":
        (actor / "strategy.py").write_text("changed")
    elif fault == "config":
        (actor / "config.json").write_text("{}")
    elif fault == "fork":
        run.context.instance_id = "another-fork"
    else:
        run.lease = SimpleNamespace(owner="wrong", generation=99)
    with pytest.raises((ValueError, subject_tests.OwnershipError)):
        execute(lane)
    assert not commands


def test_actor_residual_inventory_prevents_successful_cleanup(lane, monkeypatch):
    monkeypatch.setattr(
        actor_cleanup, "observe_actor_terminal", lambda *args: {"status": "FAIL", "balances_raw": {"WETH": "1"}}
    )
    assert execute(lane)["status"] == "FAIL"


def test_no_claimed_actor_launch_does_not_send_a_cleanup_request(run):
    result = actor_cleanup.cleanup_stimulus(
        run.context, run.store, run.lease, output=run.context.root / "actor-cleanup"
    )
    assert result["status"] == "UNMEASURED"
    assert result["reason"] == "No claimed stimulus launch"


def test_subject_reopening_during_actor_unwind_invalidates_ordered_cleanup(lane, monkeypatch):
    run, _, commands = lane
    observations = iter((run.terminal, run.opened))
    monkeypatch.setattr(actor_cleanup, "capture_position_generations", lambda *args: next(observations))
    with pytest.raises(ValueError, match="independently closed"):
        execute(lane)
    assert len(commands) == 1
    assert load_json(run.context.root / "actor-cleanup/subject-after-unwind.json") == run.opened
    assert not (run.context.root / "actor-cleanup/result.json").exists()


@pytest.mark.parametrize("quantity_status", ["FAIL", "UNMEASURED"])
def test_actor_risk_reduction_cannot_hide_missing_or_wrong_quantities(lane, monkeypatch, quantity_status):
    monkeypatch.setattr(
        actor_cleanup, "capture_stimulus_quantities", lambda *args, **kwargs: {"status": quantity_status}
    )
    result = execute(lane)
    assert result["status"] == quantity_status
    assert result["position_cleanup_status"] == "PASS"
    assert load_json(lane[0].context.root / "actor-cleanup/closure-result.json")["status"] == "PASS"


def test_post_unwind_evidence_uses_actor_inventory_block(lane, monkeypatch):
    run, _, _ = lane
    pin = (run.terminal["end_block"], run.terminal["end_block_hash"])
    calls = []

    def inventory(context, output):
        output.mkdir()
        (output / "raw.json").write_bytes(canonical({"block_number": pin[0], "block_hash": pin[1]}))
        return {"status": "PASS"}

    def positions(*args, **kwargs):
        calls.append(("positions", kwargs.get("terminal_block")))
        return run.terminal

    def quantities(*args, **kwargs):
        calls.append(("quantities", kwargs.get("terminal_block")))
        return {"status": "PASS"}

    monkeypatch.setattr(actor_cleanup, "observe_actor_terminal", inventory)
    monkeypatch.setattr(actor_cleanup, "capture_position_generations", positions)
    monkeypatch.setattr(actor_cleanup, "capture_stimulus_quantities", quantities)
    assert execute(lane)["status"] == "PASS"
    assert calls == [("positions", None), ("positions", pin), ("quantities", pin)]


@pytest.mark.parametrize("inventory", ["zero", "weth", "pending", "missing"])
def test_unclaimed_actor_is_observed_without_gaining_dispatch_authority(run, fork, monkeypatch, inventory):
    from qa_lab.chains import TOKENS

    run.context.assert_rpc_identity = fork.context.assert_rpc_identity
    run.context.public_identity = fork.context.public_identity
    if inventory == "weth":
        fork.values[TOKENS["arbitrum"]["WETH"][0].lower()] = 1
    elif inventory == "pending":
        fork.pool["pending"][anvil_default_address(1)] = {"0x1": {"hash": "0x" + "11" * 32}}
    elif inventory == "missing":
        fork.client.eth.call = lambda *args, **kwargs: b""
    monkeypatch.setattr(subject_tests.subprocess, "run", lambda *args, **kwargs: pytest.fail("unowned actor dispatch"))
    output = run.context.root / "unclaimed-observation"
    result = actor_cleanup.cleanup_stimulus(run.context, run.store, run.lease, output=output)
    expected = "FAIL" if inventory in {"weth", "pending"} else "UNMEASURED"
    assert result["status"] == expected
    assert result["dispatch_authority"] == "ABSENT"
    assert result["cleanup_disposition"] == (
        "UNOWNED_INVENTORY_REQUIRES_REVIEW" if expected == "FAIL" else "OBSERVATION_ONLY"
    )
    assert result["wallet_residuals"] == result["e2e_admission"] == "UNMEASURED"
    assert load_json(output / "result.json") == result
    assert load_json(output / "terminal/result.json") == result["terminal_inventory"]
    if inventory != "missing":
        assert load_json(output / "terminal/raw.json")["wallet"] == anvil_default_address(1)
        assert result["terminal_inventory"]["pending_transactions"] == (1 if inventory == "pending" else 0)
        assert result["terminal_inventory"]["balances_raw"]["WETH"] == ("1" if inventory == "weth" else "0")
    else:
        assert result["terminal_inventory"]["status"] == "UNMEASURED"


def test_mutated_opening_cannot_authorize_actor_unwind(lane):
    run, _, commands = lane
    path = run.context.root / "positions-open.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(subject_tests.OwnershipError, match="phase evidence changed"):
        execute(lane)
    assert commands == []


@pytest.mark.parametrize("database", ["absent", "empty", "ambiguous"])
@pytest.mark.parametrize("weth", [0, 1])
def test_claimed_actor_without_verified_boot_still_retains_inventory(run, fork, monkeypatch, database, weth):
    from qa_lab.chains import TOKENS

    token = run.store.reserve_launch(run.lease, "stimulus")
    run.store.claim_launch(run.lease, role="stimulus", token=token)
    actor = run.context.root / "actor"
    actor.mkdir()
    if database != "absent":
        with sqlite3.connect(actor / "almanak_state.db") as db:
            db.execute("CREATE TABLE strategy_state (deployment_id TEXT)")
            if database == "ambiguous":
                db.executemany(
                    "INSERT INTO strategy_state VALUES (?)",
                    [("deployment:111111111111",), ("deployment:222222222222",)],
                )
    run.context.assert_rpc_identity = fork.context.assert_rpc_identity
    run.context.public_identity = fork.context.public_identity
    fork.values[TOKENS["arbitrum"]["WETH"][0].lower()] = weth
    monkeypatch.setattr(
        subject_tests.subprocess, "run", lambda *args, **kwargs: pytest.fail("unverified actor dispatch")
    )
    output = run.context.root / "claimed-observation"
    result = actor_cleanup.cleanup_stimulus(run.context, run.store, run.lease, output=output)
    assert result["status"] == ("FAIL" if weth else "UNMEASURED")
    assert result["dispatch_authority"] == "CLAIMED_IDENTITY_UNMEASURED"
    assert result["terminal_inventory"]["balances_raw"]["WETH"] == str(weth)
    assert load_json(output / "terminal/raw.json")["wallet"] == anvil_default_address(1)
    assert load_json(output / "result.json") == result


def test_delayed_verified_boot_can_later_receive_one_cleanup_request(lane):
    run, actor, commands = lane
    database = actor / "almanak_state.db"
    saved = database.read_bytes()
    database.unlink()
    assert execute(lane, "before-boot")["status"] == "UNMEASURED"
    assert commands == []
    database.write_bytes(saved)
    assert execute(lane, "after-boot")["status"] == "PASS"
    assert len(commands) == 1


def test_cleanup_classifies_claims_after_the_cleanup_latch(run, monkeypatch):
    latch = run.store.request_cleanup

    def concurrent_claim(lease):
        token = run.store.reserve_launch(lease, "stimulus")
        run.store.claim_launch(lease, role="stimulus", token=token)
        latch(lease)

    monkeypatch.setattr(run.store, "request_cleanup", concurrent_claim)
    result = actor_cleanup.cleanup_stimulus(
        run.context,
        run.store,
        run.lease,
        output=run.context.root / "claim-before-latch",
    )
    assert result["dispatch_authority"] == "CLAIMED_IDENTITY_UNMEASURED"
    assert result["status"] == "UNMEASURED"
