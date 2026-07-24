"""Branch coverage for the runtime configs' _validate_optional_fields.

Both ``MultiChainRuntimeConfig`` and ``LocalRuntimeConfig`` carry a straight
chain of range guards over the optional tuning knobs (the local variant adds
a Solana carve-out for the gwei checks; the multi-chain variant adds the
data-freshness pair). Each guard's reject branch is exercised with a single
out-of-range value on an otherwise-valid config, plus boundary values on the
accept path. Construction bypasses __post_init__ via object.__new__ so no
env vars, keys, or RPC URLs are involved.
"""

import pytest

from almanak.framework.execution.config import (
    ConfigurationError,
    LocalRuntimeConfig,
    MultiChainRuntimeConfig,
)

_VALID_DEFAULTS = {
    "max_gas_price_gwei": 100,
    "tx_timeout_seconds": 120,
    "max_tx_value_eth": 10.0,
    "max_gas_cost_native": 0.0,
    "max_gas_cost_usd": 0.0,
    "max_slippage_bps": 0,
    "base_retry_delay": 1.0,
    "max_retry_delay": 32.0,
    "max_retries": 3,
    "data_freshness_policy": "fail_closed",
    "stale_data_threshold_seconds": 30.0,
}


def _config(**overrides) -> MultiChainRuntimeConfig:
    """Build a bare config carrying only the fields the validator reads."""
    config = object.__new__(MultiChainRuntimeConfig)
    for name, value in {**_VALID_DEFAULTS, **overrides}.items():
        setattr(config, name, value)
    return config


class TestValidateOptionalFields:
    def test_defaults_pass(self):
        _config()._validate_optional_fields()  # must not raise

    def test_boundary_values_pass(self):
        _config(
            max_gas_price_gwei=10000,
            tx_timeout_seconds=600,
            max_tx_value_eth=0.0,
            max_slippage_bps=10000,
            max_retry_delay=1.0,  # == base_retry_delay
            max_retries=0,
            data_freshness_policy="fail_open",
        )._validate_optional_fields()  # must not raise

    @pytest.mark.parametrize(
        ("field", "value", "match"),
        [
            ("max_gas_price_gwei", 0, "Must be positive"),
            ("max_gas_price_gwei", -5, "Must be positive"),
            ("max_gas_price_gwei", 10001, "Exceeds maximum"),
            ("tx_timeout_seconds", 0, "Must be positive"),
            ("tx_timeout_seconds", 601, "Exceeds maximum"),
            ("max_tx_value_eth", -0.1, "Cannot be negative"),
            ("max_gas_cost_native", -0.1, "Cannot be negative"),
            ("max_gas_cost_usd", -1.0, "Cannot be negative"),
            ("max_slippage_bps", -1, "Cannot be negative"),
            ("max_slippage_bps", 10001, "Exceeds maximum"),
            ("base_retry_delay", 0.0, "Must be positive"),
            ("max_retry_delay", 0.5, "Must be >= base_retry_delay"),
            ("max_retries", -1, "Cannot be negative"),
            ("data_freshness_policy", "fail_sometimes", "fail_closed"),
            ("stale_data_threshold_seconds", 0.0, "Must be positive"),
        ],
    )
    def test_out_of_range_value_rejected(self, field, value, match):
        with pytest.raises(ConfigurationError, match=match) as exc_info:
            _config(**{field: value})._validate_optional_fields()

        assert exc_info.value.field == field


# ---------------------------------------------------------------------------
# LocalRuntimeConfig (single-chain variant)
# ---------------------------------------------------------------------------


def _local_config(*, chain: str = "arbitrum", **overrides) -> LocalRuntimeConfig:
    """Build a bare single-chain config for the validator under test.

    The local variant has no data-freshness knobs but gates the gwei checks
    on the chain family (Solana has no gas price in gwei).
    """
    config = object.__new__(LocalRuntimeConfig)
    config.chain = chain
    fields = {key: value for key, value in _VALID_DEFAULTS.items() if not key.startswith(("data_", "stale_"))}
    fields.update(overrides)
    for name, value in fields.items():
        setattr(config, name, value)
    return config


class TestLocalValidateOptionalFields:
    def test_defaults_pass(self):
        _local_config()._validate_optional_fields()  # must not raise

    def test_boundary_values_pass(self):
        _local_config(
            max_gas_price_gwei=10000,
            tx_timeout_seconds=600,
            max_tx_value_eth=0.0,
            max_slippage_bps=10000,
            max_retry_delay=1.0,  # == base_retry_delay
            max_retries=0,
        )._validate_optional_fields()  # must not raise

    def test_solana_skips_gas_price_checks(self):
        # A gwei value that would be rejected on an EVM chain is ignored on
        # Solana (lamports, not gwei).
        _local_config(chain="solana", max_gas_price_gwei=0)._validate_optional_fields()

    @pytest.mark.parametrize(
        ("field", "value", "match"),
        [
            ("max_gas_price_gwei", 0, "Must be positive"),
            ("max_gas_price_gwei", 10001, "Exceeds maximum"),
            ("tx_timeout_seconds", 0, "Must be positive"),
            ("tx_timeout_seconds", 601, "Exceeds maximum"),
            ("max_tx_value_eth", -0.1, "Cannot be negative"),
            ("max_gas_cost_native", -0.1, "Cannot be negative"),
            ("max_gas_cost_usd", -1.0, "Cannot be negative"),
            ("max_slippage_bps", -1, "Cannot be negative"),
            ("max_slippage_bps", 10001, "Exceeds maximum"),
            ("base_retry_delay", 0.0, "Must be positive"),
            ("max_retry_delay", 0.5, "Must be >= base_retry_delay"),
            ("max_retries", -1, "Cannot be negative"),
        ],
    )
    def test_out_of_range_value_rejected(self, field, value, match):
        with pytest.raises(ConfigurationError, match=match) as exc_info:
            _local_config(**{field: value})._validate_optional_fields()

        assert exc_info.value.field == field
