"""Author hooks cannot escape process, memory or bounded output limits."""

import subprocess
import sys
from contextlib import nullcontext

import pytest

from almanak import worker_policy
from almanak.connectors.composition import connector_for
from almanak.runtime import strategy_worker
from almanak.strategy import StrategyDefinition, ValidationRequest
from almanak.worker_policy import CPU_SECONDS, MAX_BYTES, MEMORY_BYTES
from almanak.worker_session import preloaded_workers


@pytest.fixture(params=("ordinary", "preloaded", "capability"))
def author_hook(request, recorded, reference_strategy):
    mode = request.param
    if mode == "preloaded" and sys.platform != "linux":
        pytest.skip("Linux preload isolation")

    def evaluate(source):
        if mode == "capability":
            strategy = StrategyDefinition(
                filename="strategy.py", source=source + "\n" + reference_strategy.source
            )
            return connector_for(recorded.config).codec.capability.evaluate(
                strategy, recorded.config
            )
        strategy = StrategyDefinition(filename="probe.py", source=source)
        return strategy_worker(ValidationRequest(strategy=strategy, config=recorded.config))

    with preloaded_workers() if mode == "preloaded" else nullcontext():
        yield evaluate


def test_hard_limits_precede_author_source_and_cannot_be_raised(author_hook):
    author_hook(
        "import resource\n"
        f"assert resource.getrlimit(resource.RLIMIT_AS) == ({MEMORY_BYTES}, {MEMORY_BYTES})\n"
        "cpu = resource.getrlimit(resource.RLIMIT_CPU)\n"
        "usage = resource.getrusage(resource.RUSAGE_SELF)\n"
        "spent = usage.ru_utime + usage.ru_stime\n"
        f"assert cpu[0] == cpu[1] and {CPU_SECONDS} <= cpu[0] <= spent + {CPU_SECONDS} + 1\n"
        "assert resource.getrlimit(resource.RLIMIT_NPROC) == (0, 0)\n"
        "try:\n"
        "    resource.setrlimit(resource.RLIMIT_AS, (-1, -1))\n"
        "except (ValueError, OSError):\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('author raised its hard memory limit')\n"
        "try:\n"
        f"    allocation = bytearray({MEMORY_BYTES})\n"
        "except MemoryError:\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('allocation exceeded address-space limit')\n"
    )


def test_author_cannot_create_a_descendant(author_hook):
    author_hook(
        "import os\n"
        "try:\n"
        "    child = os.fork()\n"
        "except OSError:\n"
        "    pass\n"
        "else:\n"
        "    if child == 0:\n"
        "        os._exit(0)\n"
        "    os.waitpid(child, 0)\n"
        "    raise AssertionError('author created a descendant')\n"
    )


@pytest.mark.parametrize("descriptor", (1, 2))
def test_raw_output_overflow_fails_without_poisoning_next_evaluation(author_hook, descriptor):
    source = (
        f"import os; os.write({descriptor}, b'x' * {MAX_BYTES + 1}); os.write({descriptor}, b'x')"
    )
    with pytest.raises(RuntimeError, match="strategy worker (failed|output)"):
        author_hook(source)
    author_hook("pass")


def test_oversized_request_is_refused_before_launch(monkeypatch):
    from almanak.worker_process import run_worker

    def forbidden():
        raise AssertionError("oversized input must not launch a process")

    monkeypatch.setattr("almanak.worker_process.worker_command", forbidden)
    with pytest.raises(ValueError, match="input exceeds"):
        run_worker("almanak.strategy_worker", "x" * (MAX_BYTES + 1))


@pytest.mark.skipif(sys.platform != "linux", reason="Linux preload framing")
def test_bounded_result_survives_json_frame_escaping(recorded):
    from almanak.connectors.aave_v4.models import OutcomeInput
    from almanak.strategy import OutcomeRequest
    from almanak.strategy_api import Memory

    source = (
        "from almanak.strategy_api import Memory\n"
        "def on_outcome(ctx):\n"
        "    return Memory(values={'padding': chr(92) * 2500000})\n"
    )
    request = OutcomeRequest(
        strategy=StrategyDefinition(filename="probe.py", source=source),
        context=OutcomeInput(
            config=recorded.config, memory=Memory(), result=recorded.outcomes[0].strategy_result()
        ),
    )
    expected = strategy_worker(request)
    with preloaded_workers():
        assert strategy_worker(request) == expected
    assert len(Memory.model_validate_json(expected).values["padding"]) == 2500000


def test_timeout_reaps_the_actual_sandbox_process(recorded, monkeypatch):
    from almanak import worker_process

    launched = []
    popen = worker_process.subprocess.Popen

    def launch(*args, **kwargs):
        process = popen(*args, **kwargs)
        launched.append(process)
        return process

    monkeypatch.setattr(worker_process.subprocess, "Popen", launch)
    request = ValidationRequest(
        strategy=StrategyDefinition(filename="probe.py", source="import time; time.sleep(60)"),
        config=recorded.config,
    )
    with pytest.raises(worker_process.WorkerWallTimeout, match="wall-time limit"):
        worker_process.run_worker(
            "almanak.strategy_worker",
            request.model_dump_json(),
            ("--validate-config",),
            wall_seconds=2,
        )
    assert len(launched) == 1 and launched[0].returncode is not None
    assert (
        strategy_worker(
            request.model_copy(
                update={"strategy": StrategyDefinition(filename="probe.py", source="pass")}
            )
        )
        == "null"
    )


def test_policy_keeps_a_lower_host_hard_limit_and_names_a_refused_limit(monkeypatch):
    import resource

    applied = []
    hard = {resource.RLIMIT_AS: MEMORY_BYTES // 2, resource.RLIMIT_CPU: resource.RLIM_INFINITY}

    def getrlimit(kind):
        return (0, hard.get(kind, resource.RLIM_INFINITY))

    def setrlimit(kind, limits):
        if kind == resource.RLIMIT_NOFILE:
            raise ValueError("current limit exceeds maximum limit")
        applied.append((kind, limits))

    monkeypatch.setattr(worker_policy, "spent_cpu_seconds", lambda: 0)
    monkeypatch.setattr(worker_policy.resource, "getrlimit", getrlimit)
    monkeypatch.setattr(worker_policy.resource, "setrlimit", setrlimit)
    monkeypatch.setattr(worker_policy.sys, "platform", "darwin")
    with pytest.raises(RuntimeError, match="RLIMIT_NOFILE could not be applied: requested 64"):
        worker_policy.restrict_author()
    assert applied == [
        (resource.RLIMIT_CPU, (CPU_SECONDS, CPU_SECONDS)),
        (resource.RLIMIT_AS, (MEMORY_BYTES // 2, MEMORY_BYTES // 2)),
        (resource.RLIMIT_FSIZE, (MAX_BYTES, MAX_BYTES)),
    ]


@pytest.mark.skipif(sys.platform != "linux", reason="Linux resource accounting")
def test_author_cpu_budget_starts_after_trusted_startup():
    """A worker whose interpreter and SDK imports already used more CPU than the author
    budget must not be killed when the limit is applied (dashboard evaluation, 1 CPU)."""
    probe = (
        "import time, resource\n"
        "end = time.process_time() + 2.5\n"
        "while time.process_time() < end:\n"
        "    pass\n"
        "from almanak.worker_policy import restrict_author\n"
        "restrict_author(cpu_seconds=1)\n"
        "print('survived', flush=True)\n"
    )
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, timeout=60)
    assert result.returncode == 0, result
    assert result.stdout.strip() == b"survived"


def test_author_wall_budget_starts_when_startup_ends(recorded):
    """Unused startup allowance never extends the author's wall budget; this also proves
    the ready signal crosses the sandbox, since a signal at exit would never time out."""
    from almanak import worker_process

    request = ValidationRequest(
        strategy=StrategyDefinition(filename="probe.py", source="import time; time.sleep(6)"),
        config=recorded.config,
    )
    with pytest.raises(RuntimeError, match="wall-time limit"):
        worker_process.run_worker(
            "almanak.strategy_worker",
            request.model_dump_json(),
            ("--validate-config",),
            wall_seconds=2,
        )


def test_startup_has_its_own_bound(recorded, monkeypatch):
    from almanak import worker_process

    monkeypatch.setattr(worker_policy, "STARTUP_WALL_SECONDS", 0.001)
    request = ValidationRequest(
        strategy=StrategyDefinition(filename="probe.py", source="pass"), config=recorded.config
    )
    # A starved host delays startup as it delays evaluation, so the coordinator retries it
    # as a wall-time timeout instead of stopping.
    with pytest.raises(worker_process.WorkerStartupTimeout, match="startup limit") as failure:
        worker_process.run_worker(
            "almanak.strategy_worker", request.model_dump_json(), ("--validate-config",)
        )
    assert isinstance(failure.value, worker_process.WorkerWallTimeout)


# The worker as run_worker launches it, unsandboxed, with a connector import that takes 3s.
SLOW_CONNECTOR = """
import sys, time
from almanak import strategy_worker
binding = strategy_worker.request_binding
def slow(*arguments):
    time.sleep(3)
    return binding(*arguments)
strategy_worker.request_binding = slow
sys.argv = sys.argv[2:]
strategy_worker.main()
"""


def test_author_wall_budget_excludes_the_connector_import(recorded, monkeypatch):
    from almanak import worker_process

    monkeypatch.setattr(
        worker_process, "worker_command", lambda: [sys.executable, "-c", SLOW_CONNECTOR]
    )

    def evaluate(source):
        request = ValidationRequest(
            strategy=StrategyDefinition(filename="probe.py", source=source), config=recorded.config
        )
        return worker_process.run_worker(
            "almanak.strategy_worker",
            request.model_dump_json(),
            ("--validate-config",),
            wall_seconds=2,
        )

    assert evaluate("pass").strip() == "null"
    with pytest.raises(worker_process.WorkerWallTimeout, match="wall-time limit"):
        evaluate("import time; time.sleep(3)")


def test_decision_path_imports_no_backtest_or_signing_module():
    # A fresh interpreter shows what an evaluation imports; the test process has them all.
    script = """
import json, sys
import almanak.api, almanak.memory_codec, almanak.typed_parameters
from almanak.connectors.composition import CONNECTOR_IDS
from almanak.strategy_worker import prepared, request_kind
for connector in (None, *map(str, CONNECTOR_IDS)):
    body = json.dumps({"context": {"config": {"connector": connector}}})
    for flag in ("", "--outcome"):
        _, binding = prepared(flag, body)
        binding.request_type(request_kind(flag))
loaded = sys.modules.keys() & {"almanak.backtest.gateway", "almanak.gateway.service", "eth_account"}
assert not loaded, loaded
"""
    subprocess.run([sys.executable, "-c", script], check=True, timeout=60)
