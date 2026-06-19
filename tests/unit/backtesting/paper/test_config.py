"""Focused tests for paper trading configuration validation."""

from decimal import Decimal

import pytest

from almanak.framework.backtesting.paper.config import ForkLifecycle, PaperTraderConfig


def _config(**overrides):
    data = {
        "chain": "arbitrum",
        "rpc_url": "http://localhost:8545",
        "deployment_id": "paper-test",
    }
    data.update(overrides)
    return PaperTraderConfig(**data)


class TestPaperTraderConfigValidation:
    """Validation boundaries for values that affect paper balances and safety gates."""

    @pytest.mark.parametrize("bad_value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
    def test_initial_eth_rejects_non_finite_values(self, bad_value: Decimal):
        with pytest.raises(ValueError, match="initial_eth must be finite"):
            _config(initial_eth=bad_value)

    @pytest.mark.parametrize("bad_value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
    def test_initial_tokens_reject_non_finite_values(self, bad_value: Decimal):
        with pytest.raises(ValueError, match=r"initial_tokens\[USDC\] must be finite"):
            _config(initial_tokens={"USDC": bad_value})

    @pytest.mark.parametrize("bad_value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
    def test_bootstrap_rejects_non_finite_values(self, bad_value: Decimal):
        with pytest.raises(ValueError, match=r"bootstrap\[arbitrum\]\[USDC\] must be finite"):
            _config(bootstrap={"arbitrum": {"USDC": bad_value}})

    @pytest.mark.parametrize(
        ("field_name", "bad_value"),
        [
            ("oracle_divergence_threshold", Decimal("NaN")),
            ("oracle_divergence_threshold", Decimal("Infinity")),
            ("position_reconciler_tolerance_pct", Decimal("NaN")),
            ("position_reconciler_tolerance_pct", Decimal("Infinity")),
        ],
    )
    def test_thresholds_reject_non_finite_values(self, field_name: str, bad_value: Decimal):
        with pytest.raises(ValueError, match=f"{field_name} must be finite"):
            _config(**{field_name: bad_value})

    def test_persistent_lifecycle_disables_rolling_reset(self):
        config = _config(fork_lifecycle=ForkLifecycle.PERSISTENT, reset_fork_every_tick=True)

        assert config.reset_fork_every_tick is False

    def test_strict_price_mode_takes_precedence_over_allow_hardcoded_fallback_true(self):
        with pytest.warns(DeprecationWarning, match="allow_hardcoded_fallback is deprecated"):
            config = _config(allow_hardcoded_fallback=True, strict_price_mode=True)

        assert config.strict_price_mode is True

    def test_allow_hardcoded_fallback_true_alone_is_ignored(self):
        """The deprecated flag is inert: setting it alone does not relax strict mode.

        strict_price_mode is authoritative and defaults to True; relaxing requires
        strict_price_mode=False. Regression guard (PR #2889): the deprecated flag no
        longer flips the pricing mode, so a legacy allow_hardcoded_fallback=True
        cannot silently re-enable hardcoded price fallbacks.
        """
        with pytest.warns(DeprecationWarning, match="allow_hardcoded_fallback is deprecated"):
            config = _config(allow_hardcoded_fallback=True)

        assert config.strict_price_mode is True

    def test_allow_hardcoded_fallback_does_not_override_explicit_relaxed_mode(self):
        """The deprecated flag cannot force strict mode back on over an explicit relax."""
        with pytest.warns(DeprecationWarning, match="allow_hardcoded_fallback is deprecated"):
            config = _config(strict_price_mode=False, allow_hardcoded_fallback=False)

        assert config.strict_price_mode is False

    def test_initial_tokens_must_be_mapping(self):
        with pytest.raises(ValueError, match="initial_tokens must be a dict"):
            _config(initial_tokens=["USDC"])  # type: ignore[arg-type]
