"""Stimulus command validation replays owned inputs before claiming a launch."""

from types import SimpleNamespace

import pytest

from almanak.framework.anvil.accounts import anvil_default_private_key
from qa_lab import e2e_actor_worker as worker
from qa_lab.e2e_actor_binding import materialize_actor
from tests.unit.scripts import test_e2e_card as card_tests

checkout = card_tests.checkout
actor_binding = card_tests.actor_binding


@pytest.fixture
def actor(actor_binding, monkeypatch):
    args = actor_binding.arguments
    root = args["preparation"].parent
    args["preparation"].rename(root / "preparation")
    args["preparation"] = root / "preparation"
    context = args["context"]
    context.root = root
    context.rpc_url = "http://127.0.0.1:18545"
    context.require_owned = lambda path: path
    materialize_actor(output=root / "actor", **args)
    monkeypatch.setattr(
        worker, "gateway_config_from_env", lambda: SimpleNamespace(private_key=anvil_default_private_key(1))
    )
    return context, args["repo"]


def test_actor_command_uses_standard_continuous_cli_and_own_gateway(actor):
    context, repo = actor
    command = worker.actor_command(context, repo=repo, gateway_port=50097)
    assert command[:6] == ("uv", "run", "--no-sync", "almanak", "strat", "run")
    assert command[command.index("--anvil-port") + 1] == "arbitrum=18545"
    assert command[command.index("--gateway-port") + 1] == "50097"
    assert not {"--once", "--max-iterations", "--teardown-after", "--no-gateway"}.intersection(command)
    assert not any(anvil_default_private_key(1) in argument for argument in command)


@pytest.mark.parametrize("fault", ["source", "config", "old-db", "wrong-wallet"])
def test_actor_rejects_mutation_or_other_signing_wallet(actor, monkeypatch, fault):
    context, repo = actor
    if fault == "source":
        (context.root / "actor/strategy.py").write_text("changed")
    elif fault == "config":
        (context.root / "actor/config.json").write_text('{"subject_wallet":"0x' + "11" * 20 + '"}')
    elif fault == "old-db":
        (context.root / "actor/almanak_state.db").write_bytes(b"previous state")
    else:
        monkeypatch.setattr(
            worker, "gateway_config_from_env", lambda: SimpleNamespace(private_key=anvil_default_private_key(0))
        )
    reason = {
        "source": "Stimulus source differs",
        "config": "replayed binding",
        "old-db": "materialization",
        "wrong-wallet": "SDK signing configuration",
    }[fault]
    with pytest.raises(ValueError, match=reason):
        worker.actor_command(context, repo=repo, gateway_port=50097)


def test_actor_refuses_mainnet_before_any_quote_or_launch(actor):
    context, repo = actor
    context.network = "mainnet"
    with pytest.raises(ValueError, match="owned Arbitrum Anvil"):
        worker.actor_command(context, repo=repo, gateway_port=50097)


def test_actor_refuses_hosted_mode(actor, monkeypatch):
    context, repo = actor
    monkeypatch.setattr(worker, "is_local", lambda: False)
    with pytest.raises(ValueError, match="owned Arbitrum Anvil"):
        worker.actor_command(context, repo=repo, gateway_port=50097)


def test_public_actor_configuration_is_child_only_and_fork_checked(actor):
    import os

    context, _ = actor
    previous = dict(os.environ)
    configuration = worker.anvil_actor_environment(context)
    assert configuration == {
        "ALMANAK_PRIVATE_KEY": anvil_default_private_key(1),
        "ALMANAK_GATEWAY_PRIVATE_KEY": anvil_default_private_key(1),
    }
    assert dict(os.environ) == previous


def test_public_actor_configuration_refuses_unverified_fork(actor):
    context, _ = actor

    def wrong_fork():
        raise ValueError("Fork identity differs")

    context.assert_rpc_identity = wrong_fork
    with pytest.raises(ValueError, match="Fork identity"):
        worker.anvil_actor_environment(context)


def test_public_actor_configuration_refuses_mainnet(actor):
    context, _ = actor
    context.network = "mainnet"
    with pytest.raises(ValueError, match="Local Arbitrum Anvil"):
        worker.anvil_actor_environment(context)
