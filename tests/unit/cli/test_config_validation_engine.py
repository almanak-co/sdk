"""Tests for the shared config-validation engine (VIB-5986).

Covers the four legs of PR1:

1. ``strat check`` hardening — the exact incident config shape
   (``morpho_market_id: ""``, ``morpho_lltv_bps: 0``) fails with ERROR
   findings and CLI exit code 2, while comment keys and valid ids pass.
2. check/runtime parity — ``strat check`` validates the EFFECTIVE config
   (hosted ``ALMANAK_STRATEGY_CONFIG`` deep-merge applied), not the raw file.
3. The optional ``CONFIG_MODEL`` Pydantic contract — findings form for
   ``check``, raising form (``ConfigValidationError``) on the shared
   coercion path used by ``strat run`` / ``strat backtest``.
4. Runner-boot honesty — a ``ConfigValidationError`` at construction writes
   lifecycle ``ERROR`` with the stable ``CONFIG_VALIDATION_FAILED`` reason
   code and exits nonzero; generic construction crashes report
   ``STRATEGY_INIT_FAILED``.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner
from pydantic import BaseModel, ConfigDict, Field

from almanak.framework.cli import run_helpers
from almanak.framework.cli._strategy_config import coerce_strategy_config
from almanak.framework.cli.check import check, run_checks
from almanak.framework.cli.config_validation import (
    CODE_CONFIG_MODEL_VIOLATION,
    CODE_EMPTY_MARKET_IDENTITY,
    CODE_ZERO_RISK_PARAMETER,
    CONFIG_VALIDATION_FAILED,
    STRATEGY_INIT_FAILED,
    config_model_findings,
    load_effective_config,
    scan_market_identity_findings,
)
from almanak.framework.strategies.exceptions import ConfigValidationError

# A real sUSDe/USDC Morpho Blue market id (66 chars) for "valid" fixtures.
VALID_MARKET_ID = "0x85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab"

# Mirrors the AlmanakCode fail-closed template shape from the incident
# deployment: a concrete IntentStrategy that soft-HOLDs but constructs fine.
_INCIDENT_STRATEGY_SOURCE = '''
from typing import Any

from almanak.framework.strategies import IntentStrategy


class EthenaMorphoIncidentStrategy(IntentStrategy):
    """Fail-closed template: constructs on any config, HOLDs when invalid."""

    def decide(self, market: Any) -> Any:
        from almanak.framework.intents import Intent

        return Intent.hold(reason="fail-closed: verified market id required")
'''

# The incident deployment's placeholder config, reduced to the relevant keys.
_INCIDENT_CONFIG = {
    "chain": "ethereum",
    "morpho_market_id": "",
    "morpho_lltv_bps": 0,
    "target_ltv_bps": 7500,
}


def _write_fixture(tmp_path: Path, config: dict[str, Any], source: str = _INCIDENT_STRATEGY_SOURCE) -> Path:
    strategy_dir = tmp_path / "fixture_strategy"
    strategy_dir.mkdir()
    (strategy_dir / "strategy.py").write_text(source, encoding="utf-8")
    (strategy_dir / "config.json").write_text(json.dumps(config), encoding="utf-8")
    return strategy_dir


# =============================================================================
# 1. Incident-shape hardening in `strat check`
# =============================================================================


class TestIncidentConfigFailsCheck:
    def test_incident_config_produces_empty_market_identity_error(self, tmp_path: Path) -> None:
        strategy_dir = _write_fixture(tmp_path, _INCIDENT_CONFIG)
        report = run_checks(strategy_dir)

        codes = {f.code for f in report.findings}
        assert CODE_EMPTY_MARKET_IDENTITY in codes
        assert CODE_ZERO_RISK_PARAMETER in codes
        assert report.has_errors()

        empty_findings = [f for f in report.findings if f.code == CODE_EMPTY_MARKET_IDENTITY]
        assert empty_findings[0].field == "morpho_market_id"

    def test_incident_config_exits_2_via_cli(self, tmp_path: Path) -> None:
        strategy_dir = _write_fixture(tmp_path, _INCIDENT_CONFIG)
        result = CliRunner().invoke(check, ["--working-dir", str(strategy_dir), "--json"])
        assert result.exit_code == 2
        payload = json.loads(result.output)
        assert any(f["code"] == CODE_EMPTY_MARKET_IDENTITY for f in payload["findings"])

    def test_valid_market_id_produces_no_identity_finding(self, tmp_path: Path) -> None:
        config = dict(_INCIDENT_CONFIG, morpho_market_id=VALID_MARKET_ID, morpho_lltv_bps=9150)
        strategy_dir = _write_fixture(tmp_path, config)
        report = run_checks(strategy_dir)
        codes = {f.code for f in report.findings}
        assert CODE_EMPTY_MARKET_IDENTITY not in codes
        assert CODE_ZERO_RISK_PARAMETER not in codes


class TestMarketIdentityScanUnit:
    def test_null_market_id_flagged(self) -> None:
        findings = scan_market_identity_findings({"market_id": None})
        assert [f.code for f in findings] == [CODE_EMPTY_MARKET_IDENTITY]

    def test_whitespace_market_id_flagged(self) -> None:
        findings = scan_market_identity_findings({"gmx_market_id": "   "})
        assert [f.code for f in findings] == [CODE_EMPTY_MARKET_IDENTITY]

    def test_comment_keys_are_skipped(self) -> None:
        # JSON-comment convention (e.g. polymarket_signal_trader) must not flag.
        findings = scan_market_identity_findings({"_comment_market_id": ""})
        assert findings == []

    def test_nested_keys_use_dotted_path(self) -> None:
        findings = scan_market_identity_findings({"legs": {"borrow": {"market_id": ""}}})
        assert len(findings) == 1
        assert findings[0].field == "legs.borrow.market_id"

    def test_zero_lltv_is_warning_not_error(self) -> None:
        findings = scan_market_identity_findings({"morpho_lltv_bps": 0})
        assert [f.severity for f in findings] == ["warning"]

    def test_nonzero_lltv_and_absent_keys_clean(self) -> None:
        assert scan_market_identity_findings({"morpho_lltv_bps": 9150, "size_usd": 100}) == []


# =============================================================================
# 2. check/runtime parity: effective config includes the hosted env override
# =============================================================================


class TestEffectiveConfigParity:
    def test_env_override_supplying_market_id_clears_finding(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The hosted platform can fix a placeholder via the UI override —
        check must validate what the strategy actually runs with."""
        strategy_dir = _write_fixture(tmp_path, _INCIDENT_CONFIG)
        monkeypatch.setenv(
            "ALMANAK_STRATEGY_CONFIG",
            json.dumps({"morpho_market_id": VALID_MARKET_ID, "morpho_lltv_bps": 9150}),
        )
        report = run_checks(strategy_dir)
        codes = {f.code for f in report.findings}
        assert CODE_EMPTY_MARKET_IDENTITY not in codes

    def test_env_override_introducing_placeholder_is_caught(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The inverse drift: file config fine, hosted override blanks the id.
        The pre-VIB-5986 raw-file check would have passed this deployment."""
        config = dict(_INCIDENT_CONFIG, morpho_market_id=VALID_MARKET_ID, morpho_lltv_bps=9150)
        strategy_dir = _write_fixture(tmp_path, config)
        monkeypatch.setenv("ALMANAK_STRATEGY_CONFIG", json.dumps({"morpho_market_id": ""}))
        report = run_checks(strategy_dir)
        assert any(f.code == CODE_EMPTY_MARKET_IDENTITY for f in report.findings)

    def test_load_effective_config_merges_override(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        strategy_dir = _write_fixture(tmp_path, {"chain": "ethereum", "size_usd": 5})
        monkeypatch.setenv("ALMANAK_STRATEGY_CONFIG", json.dumps({"size_usd": 9}))
        config, config_path, findings = load_effective_config(strategy_dir)
        assert findings == []
        assert config_path is not None
        assert config is not None
        assert str(config["size_usd"]) == "9"

    def test_missing_file_and_no_override_is_none(self, tmp_path: Path) -> None:
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        config, config_path, findings = load_effective_config(empty_dir)
        assert config is None
        assert config_path is None
        assert findings == []


# =============================================================================
# 3. CONFIG_MODEL contract (findings form + raising form)
# =============================================================================


class _MorphoConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    morpho_market_id: str = Field(min_length=66, max_length=66)
    morpho_lltv_bps: int = Field(gt=0)


class _ModelStrategy:
    """Bare class stand-in — the engine only reads the CONFIG_MODEL attribute."""

    CONFIG_MODEL = _MorphoConfigModel


class _NoModelStrategy:
    pass


class TestConfigModelFindings:
    def test_valid_config_passes(self) -> None:
        config = {"morpho_market_id": VALID_MARKET_ID, "morpho_lltv_bps": 9150}
        assert config_model_findings(_ModelStrategy, config) == []

    def test_incident_config_violates_model(self) -> None:
        config = {"morpho_market_id": "", "morpho_lltv_bps": 0}
        findings = config_model_findings(_ModelStrategy, config)
        assert {f.field for f in findings} == {"morpho_market_id", "morpho_lltv_bps"}
        assert all(f.code == CODE_CONFIG_MODEL_VIOLATION for f in findings)
        assert all(f.severity == "error" for f in findings)

    def test_missing_required_key_is_violation(self) -> None:
        findings = config_model_findings(_ModelStrategy, {"morpho_lltv_bps": 9150})
        assert any(f.field == "morpho_market_id" for f in findings)

    def test_unknown_strategy_key_forbidden(self) -> None:
        config = {
            "morpho_market_id": VALID_MARKET_ID,
            "morpho_lltv_bps": 9150,
            "morpho_market_idd": "typo",
        }
        findings = config_model_findings(_ModelStrategy, config)
        assert any(f.field == "morpho_market_idd" for f in findings)

    def test_framework_keys_do_not_need_declaration(self) -> None:
        config = {
            "morpho_market_id": VALID_MARKET_ID,
            "morpho_lltv_bps": 9150,
            "chain": "ethereum",
            "token_funding": [{"symbol": "USDC", "amount": 100}],
            "deployment_id": "deployment:abc123",
            "anvil_funding": {"USDC": 1000},
        }
        assert config_model_findings(_ModelStrategy, config) == []

    def test_no_model_is_noop(self) -> None:
        assert config_model_findings(_NoModelStrategy, {"anything": ""}) == []

    def test_missing_config_entirely_still_flags_required_fields(self) -> None:
        # No config file and no hosted override -> config is None. A declared
        # contract with required fields must flag that, or `strat check`
        # would pass a deployment the runner refuses to boot (drift).
        findings = config_model_findings(_ModelStrategy, None)
        assert {f.field for f in findings} == {"morpho_market_id", "morpho_lltv_bps"}
        assert all(f.severity == "error" for f in findings)

    def test_missing_config_with_all_optional_model_is_clean(self) -> None:
        class _OptionalModel(BaseModel):
            size_usd: int = 5

        class _OptionalStrategy:
            CONFIG_MODEL = _OptionalModel

        assert config_model_findings(_OptionalStrategy, None) == []


class TestCoercionEnforcesConfigModel:
    def test_violation_raises_config_validation_error(self) -> None:
        with pytest.raises(ConfigValidationError) as exc_info:
            coerce_strategy_config(_ModelStrategy, {"morpho_market_id": "", "morpho_lltv_bps": 0})
        assert "CONFIG_MODEL" in str(exc_info.value)
        assert exc_info.value.field == "morpho_market_id"

    def test_valid_config_coerces(self) -> None:
        config = {"morpho_market_id": VALID_MARKET_ID, "morpho_lltv_bps": 9150}
        instance = coerce_strategy_config(_ModelStrategy, config, echo=False)
        assert instance.morpho_market_id == VALID_MARKET_ID

    def test_enforcement_can_be_disabled_for_check(self) -> None:
        # `strat check` reports the same violations as findings and must not
        # have coercion re-raise them mid-report.
        instance = coerce_strategy_config(
            _ModelStrategy,
            {"morpho_market_id": ""},
            echo=False,
            enforce_config_model=False,
        )
        assert instance.morpho_market_id == ""


# =============================================================================
# 4. Runner-boot honesty: lifecycle ERROR + nonzero exit
# =============================================================================


class _RecordingLifecycle:
    def __init__(self) -> None:
        self.requests: list[Any] = []

    def WriteState(self, request: Any) -> None:  # noqa: N802 - gRPC stub casing
        self.requests.append(request)


class _RecordingGatewayClient:
    def __init__(self) -> None:
        self.lifecycle = _RecordingLifecycle()


class _RaisingConfigStrategy:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise ConfigValidationError("morpho_market_id must be a 66-char market id", field="morpho_market_id")


class _CrashingStrategy:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("boom")


def _boot(strategy_class: type, gateway_client: Any | None) -> None:
    run_helpers._instantiate_strategy(
        strategy_class=strategy_class,
        strategy_config={"chain": "ethereum"},
        runtime_config=SimpleNamespace(chain="ethereum", execution_address="0x" + "11" * 20),
        multi_chain=False,
        strategy_chains=["ethereum"],
        chain_wallets={},
        gateway_client=gateway_client,
        deployment_id="deployment:cafecafecafe",
    )


class TestBootFailureLifecycleWrite:
    def test_config_validation_error_writes_error_state_and_exits(self) -> None:
        client = _RecordingGatewayClient()
        with pytest.raises(SystemExit) as exc_info:
            _boot(_RaisingConfigStrategy, client)
        assert exc_info.value.code == 1

        assert len(client.lifecycle.requests) == 1
        request = client.lifecycle.requests[0]
        assert request.deployment_id == "deployment:cafecafecafe"
        assert request.state == "ERROR"
        assert request.error_message.startswith(f"{CONFIG_VALIDATION_FAILED}: ")
        assert "morpho_market_id" in request.error_message

    def test_generic_init_crash_reports_strategy_init_failed(self) -> None:
        client = _RecordingGatewayClient()
        with pytest.raises(SystemExit):
            _boot(_CrashingStrategy, client)
        request = client.lifecycle.requests[0]
        assert request.state == "ERROR"
        assert request.error_message.startswith(f"{STRATEGY_INIT_FAILED}: RuntimeError: boom")

    def test_no_gateway_client_still_exits_cleanly(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            _boot(_RaisingConfigStrategy, None)
        assert exc_info.value.code == 1

    def test_lifecycle_write_failure_never_masks_boot_error(self) -> None:
        class _BrokenLifecycle:
            def WriteState(self, request: Any) -> None:  # noqa: N802
                raise ConnectionError("gateway down")

        client = SimpleNamespace(lifecycle=_BrokenLifecycle())
        with pytest.raises(SystemExit) as exc_info:
            _boot(_RaisingConfigStrategy, client)
        assert exc_info.value.code == 1
