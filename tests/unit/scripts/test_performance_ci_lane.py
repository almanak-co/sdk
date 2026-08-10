from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]


def _workflow(name: str) -> dict[str, Any]:
    return yaml.safe_load((REPO_ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8"))


def test_pytest_workflow_isolates_and_gates_performance_lane() -> None:
    workflow = _workflow("template_pytest.yml")
    jobs = workflow["jobs"]
    performance = jobs["performance"]
    aggregate = jobs["test"]

    assert performance["runs-on"] == "ubuntu-latest-l"
    run_blocks = [step.get("run", "") for step in performance["steps"]]
    serial_command = next(block for block in run_blocks if "tests/performance" in block)
    assert "tests/benchmark" in serial_command
    assert (
        "tests/unit/accounting/test_portfolio_metrics_gas_aggregator.py::test_aggregator_perf_10k_rows"
        in serial_command
    )
    assert "-n 0" in serial_command
    assert "--no-cov" in serial_command
    assert any("scripts/ci/run_benchmarks.py" in block for block in run_blocks)
    assert any("scripts/ci/compare_benchmarks.py" in block for block in run_blocks)

    assert set(aggregate["needs"]) == {"shard", "performance"}
    assert any(step.get("run") == "bash scripts/ci/assert_test_jobs_succeeded.sh" for step in aggregate["steps"])

    shard_script = (REPO_ROOT / "scripts" / "ci" / "run_pytest_shard.sh").read_text(encoding="utf-8")
    assert "--ignore=tests/performance" in shard_script
    assert "--ignore=tests/benchmark" in shard_script
    assert '-m "not integration and not benchmark"' in shard_script

    accounting_tests = (
        REPO_ROOT / "tests" / "unit" / "accounting" / "test_portfolio_metrics_gas_aggregator.py"
    ).read_text(encoding="utf-8")
    assert (
        "@pytest.mark.benchmark\n"
        "@pytest.mark.asyncio\n"
        "async def test_aggregator_perf_10k_rows" in accounting_tests
    )


def test_pr_and_main_supply_comparison_refs() -> None:
    pr = _workflow("pr.yml")
    main = _workflow("main.yml")
    suite = _workflow("template_test_suite.yml")

    assert pr["jobs"]["test_pytest"]["with"]["baseline_ref"] == "${{ github.event.pull_request.base.sha }}"
    assert main["jobs"]["test_suite"]["with"]["baseline_ref"] == "${{ github.event.before }}"
    assert suite["jobs"]["tests"]["with"]["baseline_ref"] == "${{ inputs.baseline_ref }}"


def test_aggregate_gate_rejects_failed_performance_job() -> None:
    gate = REPO_ROOT / "scripts" / "ci" / "assert_test_jobs_succeeded.sh"
    env = {**os.environ, "SHARD_RESULT": "success", "PERFORMANCE_RESULT": "failure"}

    result = subprocess.run(
        ["bash", str(gate)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 1
    assert "performance tests did not succeed" in result.stdout
