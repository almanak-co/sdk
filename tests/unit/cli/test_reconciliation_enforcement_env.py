"""Unit tests for `_reconciliation_enforcement_from_env` — the CLI env-var
bridge that lets operators opt back into fail-closed reconciliation while
VIB-3348 block-anchored balance reads are in flight.

Contract under test (see runner_models.RunnerConfig.reconciliation_enforcement
docstring):
    * Default is observation mode (False) — unset, empty, or arbitrary values
      all resolve to False.
    * Truthy opt-in values: "1", "true", "yes" — case-insensitive, surrounding
      whitespace tolerated.
    * Any other value is False. No accidental coercion (e.g. "on", "y", "2").
"""

from __future__ import annotations

import pytest

from almanak.framework.cli.run_helpers import _reconciliation_enforcement_from_env


class TestReconciliationEnforcementFromEnv:
    """Lock the env-var contract that `_build_runner` relies on."""

    def test_unset_defaults_to_observation_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ALMANAK_RECONCILIATION_ENFORCEMENT", raising=False)
        assert _reconciliation_enforcement_from_env() is False

    def test_empty_string_stays_observation_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_RECONCILIATION_ENFORCEMENT", "")
        assert _reconciliation_enforcement_from_env() is False

    @pytest.mark.parametrize("value", ["1", "true", "yes", "TRUE", "YES", "True", "Yes"])
    def test_truthy_values_enable_enforcement(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("ALMANAK_RECONCILIATION_ENFORCEMENT", value)
        assert _reconciliation_enforcement_from_env() is True

    @pytest.mark.parametrize("value", [" 1 ", "  true  ", "\tyes\n", " TRUE "])
    def test_truthy_values_tolerate_surrounding_whitespace(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("ALMANAK_RECONCILIATION_ENFORCEMENT", value)
        assert _reconciliation_enforcement_from_env() is True

    @pytest.mark.parametrize(
        "value",
        ["0", "false", "no", "FALSE", "off", "on", "y", "n", "2", "enabled", "disabled", "foo"],
    )
    def test_non_truthy_values_stay_observation_mode(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("ALMANAK_RECONCILIATION_ENFORCEMENT", value)
        assert _reconciliation_enforcement_from_env() is False
