"""VIB-4879: chain-scoped ``ALMANAK_MAX_GAS_PRICE_GWEI_<CHAIN>`` override.

The global ``ALMANAK_MAX_GAS_PRICE_GWEI`` is dead on mainnet (see the
deprecation test). The chain-scoped replacement is the escape hatch for
operators who want explicit per-chain gwei caps — e.g.
``ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON=600``. Only the chain it names is
affected; other chains see their descriptor default.

This file is the chain-scoped contract:

1. ``<prefix>MAX_GAS_PRICE_GWEI_<CHAIN_UPPER>`` is parsed at the
   ``cli_runtime`` layer (no framework env-reads).
2. The legacy unprefixed form ``MAX_GAS_PRICE_GWEI_<CHAIN_UPPER>`` is
   also accepted (parity with the other risk env vars).
3. Values exceeding ``SANE_GWEI_CEILING`` (10_000) are clamped with a
   WARNING; this guard catches misconfigured env vars.
4. Malformed / non-positive values raise ``ValueError`` at boot — a
   typo in ``.env`` should NOT silently fall back to the chain default.
5. The override is applied **per chain** in
   ``_apply_runtime_gas_risk_overrides``; an unrelated chain's
   ``tx_risk_config.max_gas_price_gwei`` is untouched.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from almanak.config.cli_runtime import chain_scoped_gwei_override


class TestChainScopedHelper:
    """Direct tests of the ``chain_scoped_gwei_override`` parser."""

    def test_returns_none_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", raising=False)
        monkeypatch.delenv("MAX_GAS_PRICE_GWEI_POLYGON", raising=False)
        assert chain_scoped_gwei_override(chain="polygon") is None

    def test_prefixed_form_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", "600")
        monkeypatch.setenv("MAX_GAS_PRICE_GWEI_POLYGON", "999")
        assert chain_scoped_gwei_override(chain="polygon") == 600

    def test_legacy_unprefixed_form_accepted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", raising=False)
        monkeypatch.setenv("MAX_GAS_PRICE_GWEI_POLYGON", "750")
        assert chain_scoped_gwei_override(chain="polygon") == 750

    @pytest.mark.parametrize(
        ("chain", "env_var"),
        [
            ("polygon", "ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON"),
            ("arbitrum", "ALMANAK_MAX_GAS_PRICE_GWEI_ARBITRUM"),
            ("base", "ALMANAK_MAX_GAS_PRICE_GWEI_BASE"),
            ("mantle", "ALMANAK_MAX_GAS_PRICE_GWEI_MANTLE"),
            ("ethereum", "ALMANAK_MAX_GAS_PRICE_GWEI_ETHEREUM"),
        ],
    )
    def test_chain_upper_suffix_resolution(self, chain: str, env_var: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """Each chain's env var follows ``..._<CHAIN_UPPER>``."""
        monkeypatch.setenv(env_var, "42")
        assert chain_scoped_gwei_override(chain=chain) == 42

    def test_lowercase_chain_normalized_to_upper(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The chain arg is uppercased before forming the env var name."""
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", "100")
        assert chain_scoped_gwei_override(chain="polygon") == 100

    def test_malformed_value_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", "not-a-number")
        with pytest.raises(ValueError, match="positive integer"):
            chain_scoped_gwei_override(chain="polygon")

    def test_zero_value_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", "0")
        with pytest.raises(ValueError, match="positive integer"):
            chain_scoped_gwei_override(chain="polygon")

    def test_negative_value_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", "-100")
        with pytest.raises(ValueError, match="positive integer"):
            chain_scoped_gwei_override(chain="polygon")

    def test_empty_value_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CodeRabbit (VIB-4879): an env set to ``""`` is an operator typo,
        not "unset" — must fail loudly rather than silently fall through to
        the chain default."""
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", "")
        with pytest.raises(ValueError, match="empty/whitespace"):
            chain_scoped_gwei_override(chain="polygon")

    def test_whitespace_value_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Whitespace-only env value is also a typo; same fail-loud rule."""
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", "   ")
        with pytest.raises(ValueError, match="empty/whitespace"):
            chain_scoped_gwei_override(chain="polygon")

    def test_empty_legacy_form_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The legacy unprefixed form is also subject to the empty-string guard
        — only when the prefixed form is unset (so we don't override the
        prefixed=value, legacy="" precedence ordering by mistake)."""
        monkeypatch.delenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", raising=False)
        monkeypatch.setenv("MAX_GAS_PRICE_GWEI_POLYGON", "")
        with pytest.raises(ValueError, match="empty/whitespace"):
            chain_scoped_gwei_override(chain="polygon")

    def test_value_above_sane_ceiling_clamps_and_warns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.execution.gas.constants import SANE_GWEI_CEILING

        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", str(SANE_GWEI_CEILING + 5000))
        mock_logger = MagicMock()
        with patch("almanak.config.cli_runtime.logger", mock_logger):
            value = chain_scoped_gwei_override(chain="polygon")
        assert value == SANE_GWEI_CEILING
        warning_messages = [str(call) for call in mock_logger.warning.call_args_list]
        assert any("SANE_GWEI_CEILING" in m and "clamping" in m for m in warning_messages), (
            f"Expected clamp warning, got: {warning_messages}"
        )

    def test_value_below_ceiling_passes_through(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", "5000")
        assert chain_scoped_gwei_override(chain="polygon") == 5000


class TestChainScopedAppliedToTxRiskConfig:
    """Integration: the override actually lands on ``tx_risk_config``
    when ``_apply_runtime_gas_risk_overrides`` runs.

    We exercise the helper directly with a fake TransactionRiskConfig and
    LocalRuntimeConfig (the real ones are validated dataclasses; the
    function only reads ``.chain`` and writes attributes by name).
    """

    @pytest.fixture
    def fake_configs(self):
        class _FakeConfig:
            chain = "polygon"
            max_gas_price_gwei = 1000  # chain default (post VIB-4879)
            max_gas_cost_native = 0.0
            max_gas_cost_usd = 0.0
            max_slippage_bps = 0

        class _FakeRisk:
            max_gas_price_gwei = 1000  # populated by TransactionRiskConfig.for_chain
            max_gas_cost_native = 0.0
            max_gas_cost_usd = 0.0
            max_slippage_bps = 0
            max_value_usd = 0

        return _FakeConfig(), _FakeRisk()

    def test_override_applied_when_chain_scoped_env_set(self, fake_configs, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.cli.run import _apply_runtime_gas_risk_overrides

        config, risk = fake_configs
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", "600")
        monkeypatch.delenv("ALMANAK_MAX_VALUE_USD", raising=False)
        monkeypatch.delenv("MAX_VALUE_USD", raising=False)
        _apply_runtime_gas_risk_overrides(risk, config)
        assert risk.max_gas_price_gwei == 600  # chain-scoped override wins

    def test_override_not_applied_for_other_chains(self, fake_configs, monkeypatch: pytest.MonkeyPatch) -> None:
        """If the env names a DIFFERENT chain, the current chain's cap is untouched."""
        from almanak.framework.cli.run import _apply_runtime_gas_risk_overrides

        config, risk = fake_configs  # chain = polygon
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI_ARBITRUM", "600")
        monkeypatch.delenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", raising=False)
        monkeypatch.delenv("ALMANAK_MAX_VALUE_USD", raising=False)
        monkeypatch.delenv("MAX_VALUE_USD", raising=False)
        _apply_runtime_gas_risk_overrides(risk, config)
        assert risk.max_gas_price_gwei == 1000  # polygon descriptor default preserved

    def test_no_override_when_env_unset(self, fake_configs, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.cli.run import _apply_runtime_gas_risk_overrides

        config, risk = fake_configs
        monkeypatch.delenv("ALMANAK_MAX_GAS_PRICE_GWEI_POLYGON", raising=False)
        monkeypatch.delenv("MAX_GAS_PRICE_GWEI_POLYGON", raising=False)
        monkeypatch.delenv("ALMANAK_MAX_VALUE_USD", raising=False)
        monkeypatch.delenv("MAX_VALUE_USD", raising=False)
        _apply_runtime_gas_risk_overrides(risk, config)
        assert risk.max_gas_price_gwei == 1000  # chain default preserved
