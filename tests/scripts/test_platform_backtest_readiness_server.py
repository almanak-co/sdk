import asyncio
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from almanak._version import __version__
from scripts import platform_backtest_readiness_server as readiness_server

app = readiness_server.app


def _payload(**overrides):
    payload = {
        "commit_sha": "a" * 40,
        "sdk_version": __version__,
        "clone_url": "https://github.com/example/strategy.git",
        "strategy_config": {},
        "backtest_config": {"start_time": "2026-01-01", "end_time": "2026-02-01"},
    }
    payload.update(overrides)
    return payload


def test_health_reports_exact_sdk_version() -> None:
    response = TestClient(app).get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "sdk_version": __version__}


def test_readiness_refuses_a_different_sdk_version_before_cloning(monkeypatch: pytest.MonkeyPatch) -> None:
    clone = Mock(side_effect=AssertionError("clone must not run"))
    monkeypatch.setattr(readiness_server.runner, "clone_strategy_repo", clone)

    response = TestClient(app).post("/v1/readiness", json=_payload(sdk_version="999.0.0"))

    assert response.status_code == 409
    assert "cannot validate requested SDK" in response.json()["detail"]
    clone.assert_not_called()


@pytest.mark.parametrize(
    "clone_url",
    (
        "file:///tmp/strategy",
        "ext::sh -c malicious",
        "https://example.com/owner/strategy.git",
        "https://github.com/owner/strategy.git?transport=file",
    ),
)
def test_readiness_refuses_non_github_clone_transports_before_cloning(
    monkeypatch: pytest.MonkeyPatch, clone_url: str
) -> None:
    clone = Mock(side_effect=AssertionError("clone must not run"))
    monkeypatch.setattr(readiness_server.runner, "clone_strategy_repo", clone)

    response = TestClient(app).post("/v1/readiness", json=_payload(clone_url=clone_url))

    assert response.status_code == 422
    clone.assert_not_called()


def test_readiness_redaction_masks_blocker_credentials_and_clone_path() -> None:
    clone_url = "https://oauth2:secret@github.com/example/strategy.git"
    clone_env = readiness_server._CloneEnv(
        commit_sha="a" * 40,
        github_clone_url=clone_url,
        strategy_dir=Path("/tmp/almanak-readiness-secret/strategy"),
    )
    raw = {
        "blockers": [
            {
                "code": "DataSourceUnavailable",
                "message": f"clone {clone_url} failed under {clone_env.strategy_dir}",
            }
        ]
    }

    safe = readiness_server.runner._redact_json_value(raw, clone_env)

    message = safe["blockers"][0]["message"]
    assert "secret" not in message
    assert "/tmp/almanak-readiness-secret" not in message
    assert "GITHUB_CLONE_URL" in message
    assert "STRATEGY_DIR" in message


@pytest.mark.asyncio
async def test_sequential_readiness_requests_launch_distinct_child_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    launches: list[tuple[str, ...]] = []

    class _Process:
        def __init__(self, pid: int) -> None:
            self.pid = pid
            self.returncode: int | None = None

        async def wait(self) -> int:
            self.returncode = 0
            return 0

    async def create_subprocess(*args: str, **_kwargs) -> _Process:
        launches.append(args)
        result_path = Path(args[-1])
        result_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "status": "ready",
                    "ready": True,
                    "blockers": [],
                    "warnings": [],
                    "checks": [],
                    "observations_checked": 1,
                }
            ),
            encoding="utf-8",
        )
        return _Process(10_000 + len(launches))

    killed: list[int] = []
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess)
    monkeypatch.setattr(readiness_server, "_kill_process_group", lambda process: killed.append(process.pid))
    request = readiness_server.ReadinessRequest.model_validate(_payload())

    first = await readiness_server._run_isolated_readiness(request)
    second = await readiness_server._run_isolated_readiness(request)

    assert first["ready"] is True
    assert second["ready"] is True
    assert len(launches) == 2
    assert launches[0][2] == launches[1][2] == "--readiness-child"
    assert launches[0][3] != launches[1][3]
    assert killed == [10_001, 10_002]


@pytest.mark.asyncio
async def test_readiness_timeout_kills_child_process_group(monkeypatch: pytest.MonkeyPatch) -> None:
    class _HungProcess:
        pid = 20_001
        returncode: int | None = None
        waits = 0

        async def wait(self) -> int:
            self.waits += 1
            if self.waits == 1:
                await asyncio.Event().wait()
            self.returncode = -9
            return -9

    process = _HungProcess()

    async def create_subprocess(*_args, **_kwargs) -> _HungProcess:
        return process

    killed: list[int] = []
    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess)
    monkeypatch.setattr(readiness_server, "_READINESS_CHILD_TIMEOUT_SECONDS", 0.001)
    monkeypatch.setattr(readiness_server, "_kill_process_group", lambda child: killed.append(child.pid))
    request = readiness_server.ReadinessRequest.model_validate(_payload())

    result = await readiness_server._run_isolated_readiness(request)

    assert result["ready"] is False
    assert result["blockers"][0]["code"] == "ReadinessTimeout"
    assert process.waits == 2
    assert killed == [20_001, 20_001]
