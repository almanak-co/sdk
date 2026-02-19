"""Tests for scripts/test_demo_strategies_gateway.py."""

import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _load_gateway_script_module():
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "test_demo_strategies_gateway.py"
    spec = importlib.util.spec_from_file_location("test_demo_strategies_gateway_script", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load script module from {script_path}")  # noqa: TRY003
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_start_gateway_sets_insecure_flag():
    """Gateway test harness should set insecure mode for local anvil usage."""
    module = _load_gateway_script_module()
    tester = module.GatewayStrategyTester(Path.cwd())

    captured_env = {}

    class _Proc:
        def poll(self):
            return None

    def _fake_popen(*_args, **kwargs):
        captured_env.update(kwargs["env"])
        return _Proc()

    with patch.object(module.subprocess, "Popen", side_effect=_fake_popen):
        with patch.object(module.asyncio, "sleep", new=AsyncMock(return_value=None)):
            ok = await tester.start_gateway("arbitrum")

    assert ok is True
    assert captured_env["ALMANAK_GATEWAY_ALLOW_INSECURE"] == "True"


@pytest.mark.asyncio
async def test_start_gateway_returns_false_and_prints_output_on_early_exit():
    """Gateway startup should fail clearly when process exits immediately."""
    module = _load_gateway_script_module()
    tester = module.GatewayStrategyTester(Path.cwd())

    proc = MagicMock()
    proc.poll.return_value = 1
    proc.stdout.read.return_value = b"fatal startup error"

    with patch.object(module.subprocess, "Popen", return_value=proc):
        with patch.object(module.asyncio, "sleep", new=AsyncMock(return_value=None)):
            ok = await tester.start_gateway("arbitrum")

    assert ok is False
