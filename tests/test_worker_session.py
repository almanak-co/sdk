"""Preloading preserves fresh process state and denies cross-evaluation channels."""

import subprocess
import sys

import pytest

from almanak.runtime import strategy_worker
from almanak.strategy import StrategyDefinition, ValidationRequest
from almanak.worker_session import preloaded_workers
from tests.test_economic_math import reserve as reserve
from tests.test_economic_model import simulation as simulation

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux preload isolation")


def validate(config, source):
    return strategy_worker(
        ValidationRequest(
            config=config,
            strategy=StrategyDefinition(filename="strategy.py", source=source),
        )
    )


def test_fresh_child_cannot_mutate_next_worker_or_parent(recorded):
    with preloaded_workers():
        assert validate(recorded.config, "import os; os.preload_poison = True") == "null"
        assert (
            validate(recorded.config, "import os; assert not hasattr(os, 'preload_poison')")
            == "null"
        )
        for source in (
            "import os; os.kill(os.getppid(), 9)",
            "import os; os.fork()",
            "import socket; socket.socket()",
            "import socket; socket.socketpair()",
            "open('/preload-poison', 'w')",
            "open('/proc/self/mem', 'rb')",
            "import ctypes; lib=ctypes.CDLL(None); assert lib.ptrace(16, 1, 0, 0) == 0",
            "import ctypes; lib=ctypes.CDLL(None); assert lib.shmget(12345, 4096, 0o1666) >= 0",
        ):
            with pytest.raises(RuntimeError, match="strategy worker failed"):
                validate(recorded.config, source)
            assert validate(recorded.config, "pass") == "null"


def test_preloaded_reference_matches_fresh_execution(simulation, tmp_path):
    from almanak.backtest.economic_state import EconomicState
    from almanak.backtest.engine import Journal, Simulator

    journal, model = simulation
    initial = journal.load()
    expected = Simulator(journal, model).run()
    other = Journal(tmp_path / "preloaded.sqlite", EconomicState)
    other.create(initial)
    with preloaded_workers():
        result = Simulator(other, model).run()
    assert result == expected


def test_preload_imports_only_the_requested_trusted_connector():
    # A fresh interpreter shows what the preload parent itself imports; the
    # test process has already loaded every connector.
    script = """
import json, sys
from almanak.worker_server import dispatch, preload
target = "almanak.connectors.uniswap_v4.connector"
other = "almanak.connectors.gmx_v2.connector"
assert target not in sys.modules and other not in sys.modules
preload({"body": "not json", "arguments": ["--economic"]})
preload({"body": "{}"})
preload({"body": json.dumps({"context": {"config": {"connector": "bogus"}}}), "arguments": []})
body = json.dumps({"context": {"config": {"connector": "uniswap_v4"}}})
# The forked child rejects this body; only the parent's preload can import here.
assert "error" in dispatch({"body": body, "arguments": ["--economic"]})
assert target in sys.modules and other not in sys.modules
# The parent also holds the configuration types the strategy API dispatches on, so a
# forked evaluation does not import them; they bring no other connector with them.
assert "almanak.connectors.composed.fork" in sys.modules
assert "almanak.connectors.gmx_v2.fork" in sys.modules
"""
    subprocess.run([sys.executable, "-c", script], check=True, timeout=60)


def forbidden(*_, **__):
    raise AssertionError("a preloaded session must serve this evaluation")


def test_check_hooks_share_one_preloaded_worker(recorded, reference_strategy, monkeypatch):
    from almanak import worker_session
    from almanak.backtest.dependencies import DataDependencies
    from almanak.checks import check_parameters
    from almanak.configuration.parameter_codec import hook_parameters

    starts = []
    start = worker_session.WorkerSession.start

    def counted(session):
        starts.append(session)
        return start(session)

    monkeypatch.setattr(worker_session.WorkerSession, "start", counted)
    monkeypatch.setattr("almanak.worker_process.run_worker", forbidden)
    monkeypatch.setattr("almanak.runtime.run_worker", forbidden)
    stages: list[dict] = []
    capabilities, dependencies = check_parameters(
        hook_parameters(recorded.config), reference_strategy, stages
    )
    assert capabilities == recorded.capabilities
    # The reference declares no data dependencies, so its hook result is the default.
    assert dependencies == DataDependencies().model_dump(mode="json")
    assert [s["stage"] for s in stages] == [
        "sandbox",
        "strategy",
        "capabilities",
        "data_dependencies",
    ]
    assert {s["status"] for s in stages} == {"passed"}
    assert len(starts) == 1 and starts[0].process.poll() is not None


def test_unstartable_preload_worker_fails_the_sandbox_stage(
    recorded, reference_strategy, monkeypatch
):
    from almanak.checks import CheckFailure, check_parameters
    from almanak.configuration.parameter_codec import hook_parameters

    message = "bwrap: No permissions to create a new namespace"
    monkeypatch.setattr(
        "almanak.worker_session.worker_command",
        lambda: ["/bin/sh", "-c", f"echo '{message}' >&2"],
    )
    stages: list[dict] = []
    with pytest.raises(CheckFailure, match="sandboxed preload worker ended") as failure:
        check_parameters(hook_parameters(recorded.config), reference_strategy, stages)
    detail = failure.value.diagnostic()
    assert detail["code"] == "check_stage_failed" and detail["field"] == "sandbox"
    assert message in detail["message"]
    states = {s["stage"]: s["status"] for s in detail["stages"]}
    assert states["sandbox"] == "failed"
    assert states["strategy"] == states["capabilities"] == "not_checked"


def test_failed_hook_request_fails_its_own_stage(recorded, reference_strategy):
    from almanak.checks import CheckFailure, check_parameters
    from almanak.configuration.parameter_codec import hook_parameters

    definition = StrategyDefinition(
        filename="strategy.py",
        source=reference_strategy.source
        + "\ndef capabilities(config):\n    raise ValueError('no')\n",
    )
    with pytest.raises(CheckFailure, match="strategy worker failed") as failure:
        check_parameters(hook_parameters(recorded.config), definition)
    states = {s["stage"]: s["status"] for s in failure.value.diagnostic()["stages"]}
    assert states["sandbox"] == states["strategy"] == "passed"
    assert states["capabilities"] == "failed"
    assert states["data_dependencies"] == "not_checked"


def test_capability_hook_in_session_matches_its_own_worker(recorded, reference_strategy):
    from almanak.runtime import strategy_capabilities

    with preloaded_workers():
        assert strategy_capabilities(reference_strategy, recorded.config) == recorded.capabilities
        broken = StrategyDefinition(
            filename="strategy.py", source="def capabilities(config):\n    raise ValueError('x')\n"
        )
        with pytest.raises(RuntimeError, match="strategy worker failed"):
            strategy_capabilities(broken, recorded.config)
        assert strategy_capabilities(reference_strategy, recorded.config) == recorded.capabilities


def test_started_worker_is_claimed_once_and_stopped(recorded, monkeypatch):
    from almanak.worker_session import ACTIVE, STARTED, started_workers

    with started_workers():
        started = STARTED.get()
        assert started is not None
        with preloaded_workers():
            assert ACTIVE.get() is started
            assert validate(recorded.config, "pass") == "null"
        assert STARTED.get() is None
        with preloaded_workers():
            assert ACTIVE.get() not in (None, started)
    assert started.process.poll() is not None
    with started_workers():
        unclaimed = STARTED.get()
    assert unclaimed is not None and unclaimed.process.poll() is not None
    assert STARTED.get() is None

    def unavailable():
        raise RuntimeError("Linux strategy execution requires bubblewrap (bwrap)")

    monkeypatch.setattr("almanak.worker_session.worker_command", unavailable)
    with started_workers():
        assert STARTED.get() is None
        with pytest.raises(RuntimeError, match="requires bubblewrap"), preloaded_workers():
            pass


def test_preload_worker_refuses_an_unknown_module():
    from almanak.worker_server import dispatch

    assert dispatch({"module": "os", "body": "{}", "arguments": []}) == {
        "error": "preload worker request names an unknown module"
    }
