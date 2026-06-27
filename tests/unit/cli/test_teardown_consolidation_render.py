"""VIB-5011 — CLI rendering of the token-consolidation summary.

Covers `_render_consolidation_summary` / `_consolidation_payload` /
`_echo_warnings` branch-by-branch, plus the `status` command's terminal
consolidation rendering (the CRAP-gate driver: these paths previously had no
direct tests).
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import click
import pytest
from click.testing import CliRunner

from almanak.framework.cli.teardown import (
    _consolidation_payload,
    _echo_warnings,
    _render_consolidation_summary,
    _render_verification_status,
    status,
)
from almanak.framework.teardown.models import (
    TeardownAssetPolicy,
    TeardownMode,
    TeardownRequest,
    TeardownStatus,
)


def _render_output(manager) -> str:
    """Run the renderer inside a click context and capture stdout."""
    runner = CliRunner()

    @click.command()
    def _cmd():
        _render_consolidation_summary(manager, "deployment:abc")

    result = runner.invoke(_cmd, [])
    assert result.exit_code == 0
    return result.output


def _manager_with(payload):
    manager = MagicMock(name="state_manager")
    manager.get_result_payload = MagicMock(return_value=payload)
    return manager


class TestConsolidationPayload:
    def test_manager_without_accessor_yields_none(self):
        manager = SimpleNamespace()  # no get_result_payload at all
        assert _consolidation_payload(manager, "d") is None

    def test_accessor_raising_yields_none(self):
        manager = MagicMock()
        manager.get_result_payload.side_effect = RuntimeError("db locked")
        assert _consolidation_payload(manager, "d") is None

    def test_none_payload_yields_none(self):
        assert _consolidation_payload(_manager_with(None), "d") is None

    def test_non_dict_consolidation_yields_none(self):
        assert _consolidation_payload(_manager_with({"consolidation": "oops"}), "d") is None

    def test_dict_consolidation_returned(self):
        consolidation = {"planned": 1, "succeeded": 1, "failed": 0}
        payload = {"consolidation": consolidation}
        assert _consolidation_payload(_manager_with(payload), "d") == consolidation


class TestRenderConsolidationSummary:
    def test_silent_when_no_payload(self):
        assert _render_output(SimpleNamespace()) == ""

    def test_failed_branch_warns_and_lists_warnings(self):
        manager = _manager_with(
            {
                "consolidation": {
                    "planned": 2,
                    "succeeded": 1,
                    "failed": 1,
                    "warnings": ["swap reverted at max slippage"],
                    "target_token": "USDC",
                }
            }
        )
        out = _render_output(manager)
        assert "WARNING: 1 consolidation swap(s) failed" in out
        assert "residual non-target tokens" in out
        assert "swap reverted at max slippage" in out

    def test_success_branch_prints_summary_and_warnings(self):
        """Warnings ride along with success — the wallet-scope disclosure
        must be visible even when every swap landed (CodeRabbit review)."""
        manager = _manager_with(
            {
                "consolidation": {
                    "planned": 1,
                    "succeeded": 1,
                    "failed": 0,
                    "warnings": ["consolidation amounts are wallet-scoped (amount=all) for: WETH"],
                    "target_token": "USDC",
                }
            }
        )
        out = _render_output(manager)
        assert "consolidated 1 token(s) → USDC" in out
        assert "wallet-scoped" in out

    def test_success_branch_falls_back_to_generic_target_label(self):
        manager = _manager_with({"consolidation": {"planned": 1, "succeeded": 1, "failed": 0, "warnings": []}})
        out = _render_output(manager)
        assert "consolidated 1 token(s) → target token" in out

    def test_nothing_planned_prints_skip_warnings(self):
        manager = _manager_with(
            {
                "consolidation": {
                    "planned": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "warnings": ["emergency_mode: HARD teardown skips token consolidation"],
                }
            }
        )
        out = _render_output(manager)
        assert "consolidation: emergency_mode" in out

    def test_nothing_planned_no_warnings_is_silent(self):
        manager = _manager_with({"consolidation": {"planned": 0, "succeeded": 0, "failed": 0, "warnings": []}})
        assert _render_output(manager) == ""


class TestRenderVerificationStatus:
    """VIB-2932 / VIB-5472 — CLI surfacing of closure-verification confidence."""

    @staticmethod
    def _render(manager) -> str:
        runner = CliRunner()

        @click.command()
        def _cmd():
            _render_verification_status(manager, "deployment:abc")

        result = runner.invoke(_cmd, [])
        assert result.exit_code == 0
        return result.output

    def test_chain_verified_shown(self):
        out = self._render(_manager_with({"verification_status": "chain_verified"}))
        assert "chain_verified" in out

    def test_unverified_flagged_loud(self):
        out = self._render(_manager_with({"verification_status": "unverified"}))
        assert "UNVERIFIED" in out
        assert "NOT chain-confirmed" in out

    def test_failed_shown(self):
        out = self._render(_manager_with({"verification_status": "failed"}))
        assert "FAILED" in out

    def test_not_run_is_silent(self):
        assert self._render(_manager_with({"verification_status": "not_run"})) == ""

    def test_missing_field_is_silent(self):
        assert self._render(_manager_with({})) == ""

    def test_no_accessor_is_silent(self):
        assert self._render(SimpleNamespace()) == ""

    def test_accessor_raising_is_silent(self):
        manager = MagicMock()
        manager.get_result_payload.side_effect = RuntimeError("db locked")
        assert self._render(manager) == ""


class TestEchoWarnings:
    def test_caps_at_five(self):
        runner = CliRunner()

        @click.command()
        def _cmd():
            _echo_warnings([f"w{i}" for i in range(8)], "- ")

        out = runner.invoke(_cmd, []).output
        assert out.count("- w") == 5


def _request(status_value: TeardownStatus) -> TeardownRequest:
    return TeardownRequest(
        deployment_id="deployment:abc",
        mode=TeardownMode.SOFT,
        asset_policy=TeardownAssetPolicy.TARGET_TOKEN,
        target_token="USDC",
        requested_by="test",
        reason="unit test",
        status=status_value,
        requested_at=datetime.now(UTC),
    )


class TestStatusCommandConsolidationRender:
    """`status` renders the consolidation summary only on COMPLETED."""

    @pytest.fixture
    def patched_env(self, monkeypatch, tmp_path):
        import almanak.framework.cli.teardown as mod

        manager = MagicMock(name="state_manager")
        monkeypatch.setattr(mod, "_resolve_and_export_strategy_folder", lambda wd: str(tmp_path))
        monkeypatch.setattr(mod, "_get_teardown_state_manager_or_die", lambda: manager)
        return manager

    def test_completed_status_renders_consolidation(self, patched_env):
        manager = patched_env
        manager.get_request.return_value = _request(TeardownStatus.COMPLETED)
        manager.get_result_payload.return_value = {
            "consolidation": {"planned": 1, "succeeded": 1, "failed": 0, "warnings": [], "target_token": "USDC"}
        }

        result = CliRunner().invoke(status, ["-s", "deployment:abc"])

        assert result.exit_code == 0
        assert "Teardown Status" in result.output
        assert "consolidated 1 token(s) → USDC" in result.output

    def test_completed_status_renders_verification(self, patched_env):
        """VIB-2932 / VIB-5472: the plain `status` command surfaces the
        closure-verification confidence on COMPLETED rows, not just `--wait`."""
        manager = patched_env
        manager.get_request.return_value = _request(TeardownStatus.COMPLETED)
        manager.get_result_payload.return_value = {
            "verification_status": "unverified",
            "consolidation": {"planned": 0, "succeeded": 0, "failed": 0, "warnings": []},
        }

        result = CliRunner().invoke(status, ["-s", "deployment:abc"])

        assert result.exit_code == 0
        assert "UNVERIFIED" in result.output

    def test_non_completed_status_skips_consolidation(self, patched_env):
        manager = patched_env
        manager.get_request.return_value = _request(TeardownStatus.EXECUTING)

        result = CliRunner().invoke(status, ["-s", "deployment:abc"])

        assert result.exit_code == 0
        manager.get_result_payload.assert_not_called()
        assert "consolidated" not in result.output

    def test_no_request_found(self, patched_env):
        manager = patched_env
        manager.get_request.return_value = None

        result = CliRunner().invoke(status, ["-s", "deployment:abc"])

        assert result.exit_code == 0
        assert "No teardown request found" in result.output

    def test_json_mode_bypasses_render(self, patched_env):
        manager = patched_env
        request = _request(TeardownStatus.COMPLETED)
        manager.get_request.return_value = request

        result = CliRunner().invoke(status, ["-s", "deployment:abc", "--json"])

        assert result.exit_code == 0
        manager.get_result_payload.assert_not_called()
        assert '"deployment_id"' in result.output
