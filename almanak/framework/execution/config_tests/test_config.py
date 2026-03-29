"""Tests for LocalRuntimeConfig.

This module tests the configuration class for local execution environment,
including field validation, environment variable loading, and security
requirements.
"""

import os
from unittest.mock import patch

import pytest

from almanak.framework.execution.config import (
    CHAIN_IDS,
    ConfigurationError,
    LocalRuntimeConfig,
    MissingEnvironmentVariableError,
)

# Test private key (Ganache default account #0 - DO NOT USE IN PRODUCTION)
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_WALLET_ADDRESS = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Alternative test key for different address tests
TEST_PRIVATE_KEY_2 = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"


class TestLocalRuntimeConfigInit:
    """Tests for LocalRuntimeConfig initialization."""

    def test_init_with_valid_config(self):
        """Test initialization with valid configuration."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=TEST_PRIVATE_KEY,
        )

        assert config.chain == "arbitrum"
        assert config.chain_id == 42161
        assert config.wallet_address == TEST_WALLET_ADDRESS
        assert config.max_gas_price_gwei == 100  # default
        assert config.tx_timeout_seconds == 120  # default
        assert config.simulation_enabled is False  # default

    def test_init_normalizes_chain_to_lowercase(self):
        """Test that chain name is normalized to lowercase."""
        config = LocalRuntimeConfig(
            chain="ARBITRUM",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=TEST_PRIVATE_KEY,
        )
        assert config.chain == "arbitrum"

    def test_init_with_custom_optional_fields(self):
        """Test initialization with custom optional field values."""
        config = LocalRuntimeConfig(
            chain="ethereum",
            rpc_url="https://mainnet.infura.io/v3/key",
            private_key=TEST_PRIVATE_KEY,
            max_gas_price_gwei=50,
            tx_timeout_seconds=60,
            simulation_enabled=True,
            max_tx_value_eth=5.0,
            max_retries=5,
        )

        assert config.max_gas_price_gwei == 50
        assert config.tx_timeout_seconds == 60
        assert config.simulation_enabled is True
        assert config.max_tx_value_eth == 5.0
        assert config.max_retries == 5

    def test_init_derives_wallet_address(self):
        """Test that wallet address is derived from private key."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=TEST_PRIVATE_KEY,
        )
        assert config.wallet_address == TEST_WALLET_ADDRESS

    def test_init_private_key_without_0x_prefix(self):
        """Test initialization with private key without 0x prefix."""
        key_no_prefix = TEST_PRIVATE_KEY[2:]  # Remove 0x
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=key_no_prefix,
        )
        assert config.wallet_address == TEST_WALLET_ADDRESS


class TestChainValidation:
    """Tests for chain field validation."""

    def test_empty_chain_raises_error(self):
        """Test that empty chain raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key=TEST_PRIVATE_KEY,
            )
        assert exc_info.value.field == "chain"
        assert "empty" in exc_info.value.reason.lower()

    def test_unsupported_chain_raises_error(self):
        """Test that unsupported chain raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="unsupported_chain",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key=TEST_PRIVATE_KEY,
            )
        assert exc_info.value.field == "chain"
        assert "unsupported" in exc_info.value.reason.lower()

    @pytest.mark.parametrize("chain", CHAIN_IDS.keys())
    def test_all_supported_chains(self, chain: str):
        """Test that all chains in CHAIN_IDS are valid."""
        config = LocalRuntimeConfig(
            chain=chain,
            rpc_url="https://rpc.example.com",
            private_key=TEST_PRIVATE_KEY,
        )
        assert config.chain == chain
        assert config.chain_id == CHAIN_IDS[chain]


class TestRPCURLValidation:
    """Tests for RPC URL field validation."""

    def test_empty_rpc_url_raises_error(self):
        """Test that empty RPC URL raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="",
                private_key=TEST_PRIVATE_KEY,
            )
        assert exc_info.value.field == "rpc_url"
        assert "empty" in exc_info.value.reason.lower()

    def test_invalid_rpc_url_raises_error(self):
        """Test that invalid RPC URL format raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="not-a-valid-url",
                private_key=TEST_PRIVATE_KEY,
            )
        assert exc_info.value.field == "rpc_url"
        assert "invalid" in exc_info.value.reason.lower()

    def test_http_url_is_valid(self):
        """Test that http:// URLs are valid."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="http://localhost:8545",
            private_key=TEST_PRIVATE_KEY,
        )
        assert config.rpc_url == "http://localhost:8545"

    def test_https_url_is_valid(self):
        """Test that https:// URLs are valid."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=TEST_PRIVATE_KEY,
        )
        assert config.rpc_url == "https://arb1.arbitrum.io/rpc"

    def test_url_with_port_is_valid(self):
        """Test that URLs with port are valid."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="http://localhost:8545",
            private_key=TEST_PRIVATE_KEY,
        )
        assert config.rpc_url == "http://localhost:8545"

    def test_url_with_path_is_valid(self):
        """Test that URLs with path are valid."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://mainnet.infura.io/v3/abc123",
            private_key=TEST_PRIVATE_KEY,
        )
        assert config.rpc_url == "https://mainnet.infura.io/v3/abc123"


class TestPrivateKeyValidation:
    """Tests for private key field validation."""

    def test_empty_private_key_raises_error(self):
        """Test that empty private key raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key="",
            )
        assert exc_info.value.field == "private_key"
        assert "empty" in exc_info.value.reason.lower()

    def test_short_private_key_raises_error(self):
        """Test that short private key raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key="0x1234",
            )
        assert exc_info.value.field == "private_key"

    def test_invalid_hex_private_key_raises_error(self):
        """Test that invalid hex private key raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key="0x" + "xyz" * 21 + "ab",  # Invalid hex chars
            )
        assert exc_info.value.field == "private_key"


class TestOptionalFieldValidation:
    """Tests for optional field validation."""

    def test_zero_max_gas_price_raises_error(self):
        """Test that zero max gas price raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key=TEST_PRIVATE_KEY,
                max_gas_price_gwei=0,
            )
        assert exc_info.value.field == "max_gas_price_gwei"

    def test_negative_max_gas_price_raises_error(self):
        """Test that negative max gas price raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key=TEST_PRIVATE_KEY,
                max_gas_price_gwei=-10,
            )
        assert exc_info.value.field == "max_gas_price_gwei"

    def test_excessive_max_gas_price_raises_error(self):
        """Test that excessive max gas price raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key=TEST_PRIVATE_KEY,
                max_gas_price_gwei=10001,
            )
        assert exc_info.value.field == "max_gas_price_gwei"
        assert "exceeds" in exc_info.value.reason.lower()

    def test_zero_timeout_raises_error(self):
        """Test that zero timeout raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key=TEST_PRIVATE_KEY,
                tx_timeout_seconds=0,
            )
        assert exc_info.value.field == "tx_timeout_seconds"

    def test_excessive_timeout_raises_error(self):
        """Test that excessive timeout raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key=TEST_PRIVATE_KEY,
                tx_timeout_seconds=601,
            )
        assert exc_info.value.field == "tx_timeout_seconds"

    def test_negative_max_tx_value_raises_error(self):
        """Test that negative max tx value raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key=TEST_PRIVATE_KEY,
                max_tx_value_eth=-1.0,
            )
        assert exc_info.value.field == "max_tx_value_eth"

    def test_max_retry_delay_less_than_base_raises_error(self):
        """Test that max retry delay < base delay raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key=TEST_PRIVATE_KEY,
                base_retry_delay=10.0,
                max_retry_delay=5.0,
            )
        assert exc_info.value.field == "max_retry_delay"

    def test_negative_max_retries_raises_error(self):
        """Test that negative max retries raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig(
                chain="arbitrum",
                rpc_url="https://arb1.arbitrum.io/rpc",
                private_key=TEST_PRIVATE_KEY,
                max_retries=-1,
            )
        assert exc_info.value.field == "max_retries"


class TestFromEnv:
    """Tests for from_env() class method."""

    def test_from_env_loads_required_fields(self):
        """Test that from_env loads required fields from environment."""
        env_vars = {
            "ALMANAK_CHAIN": "arbitrum",
            "ALMANAK_RPC_URL": "https://arb1.arbitrum.io/rpc",
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
        }
        with patch.dict(os.environ, env_vars, clear=False):
            config = LocalRuntimeConfig.from_env()

        assert config.chain == "arbitrum"
        assert config.rpc_url == "https://arb1.arbitrum.io/rpc"
        assert config.wallet_address == TEST_WALLET_ADDRESS

    def test_from_env_loads_optional_fields(self):
        """Test that from_env loads optional fields from environment."""
        env_vars = {
            "ALMANAK_CHAIN": "ethereum",
            "ALMANAK_RPC_URL": "https://mainnet.infura.io",
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
            "ALMANAK_MAX_GAS_PRICE_GWEI": "50",
            "ALMANAK_TX_TIMEOUT_SECONDS": "60",
            "ALMANAK_SIMULATION_ENABLED": "true",
            "ALMANAK_MAX_TX_VALUE_ETH": "5.0",
            "ALMANAK_MAX_RETRIES": "5",
        }
        with patch.dict(os.environ, env_vars, clear=False):
            config = LocalRuntimeConfig.from_env()

        assert config.max_gas_price_gwei == 50
        assert config.tx_timeout_seconds == 60
        assert config.simulation_enabled is True
        assert config.max_tx_value_eth == 5.0
        assert config.max_retries == 5

    def test_from_env_missing_required_raises_error(self):
        """Test that missing required env var raises error."""
        env_vars = {
            "ALMANAK_CHAIN": "arbitrum",
            # Missing ALMANAK_RPC_URL
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
        }
        with patch.dict(os.environ, env_vars, clear=True):
            with pytest.raises(MissingEnvironmentVariableError) as exc_info:
                LocalRuntimeConfig.from_env()
            assert "ALMANAK_RPC_URL" in str(exc_info.value)

    def test_from_env_custom_prefix(self):
        """Test from_env with custom prefix."""
        env_vars = {
            "MY_CHAIN": "polygon",
            "MY_RPC_URL": "https://polygon-rpc.com",
            "MY_PRIVATE_KEY": TEST_PRIVATE_KEY,
        }
        with patch.dict(os.environ, env_vars, clear=False):
            config = LocalRuntimeConfig.from_env(prefix="MY_")

        assert config.chain == "polygon"
        assert config.rpc_url == "https://polygon-rpc.com"

    def test_from_env_boolean_values(self):
        """Test that boolean values are parsed correctly."""
        test_cases = [
            ("true", True),
            ("True", True),
            ("TRUE", True),
            ("1", True),
            ("yes", True),
            ("y", True),
            ("false", False),
            ("False", False),
            ("0", False),
            ("no", False),
        ]

        for value, expected in test_cases:
            env_vars = {
                "ALMANAK_CHAIN": "arbitrum",
                "ALMANAK_RPC_URL": "https://arb1.arbitrum.io/rpc",
                "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
                "ALMANAK_SIMULATION_ENABLED": value,
            }
            with patch.dict(os.environ, env_vars, clear=False):
                config = LocalRuntimeConfig.from_env()
                assert config.simulation_enabled is expected, f"Failed for value: {value}"

    def test_from_env_invalid_integer_raises_error(self):
        """Test that invalid integer value raises ConfigurationError."""
        env_vars = {
            "ALMANAK_CHAIN": "arbitrum",
            "ALMANAK_RPC_URL": "https://arb1.arbitrum.io/rpc",
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
            "ALMANAK_MAX_GAS_PRICE_GWEI": "not-an-integer",
        }
        with patch.dict(os.environ, env_vars, clear=False):
            with pytest.raises(ConfigurationError) as exc_info:
                LocalRuntimeConfig.from_env()
            assert "ALMANAK_MAX_GAS_PRICE_GWEI" in exc_info.value.field

    # VIB-303: Chain-specific gas price cap defaults
    # Note: load_dotenv is patched to prevent the real .env file from polluting test defaults.
    def test_from_env_polygon_uses_500_gwei_default(self):
        """Polygon should use 500 gwei by default (not 100)."""
        env_vars = {
            "ALMANAK_CHAIN": "polygon",
            "ALMANAK_RPC_URL": "https://polygon-rpc.com",
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
        }
        with patch("almanak.framework.execution.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                config = LocalRuntimeConfig.from_env()
        assert config.max_gas_price_gwei == 500

    def test_from_env_arbitrum_uses_10_gwei_default(self):
        """Arbitrum should use 10 gwei by default (L2 is cheap)."""
        env_vars = {
            "ALMANAK_CHAIN": "arbitrum",
            "ALMANAK_RPC_URL": "https://arb1.arbitrum.io/rpc",
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
        }
        with patch("almanak.framework.execution.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                config = LocalRuntimeConfig.from_env()
        assert config.max_gas_price_gwei == 10

    def test_from_env_ethereum_uses_300_gwei_default(self):
        """Ethereum should use 300 gwei by default."""
        env_vars = {
            "ALMANAK_CHAIN": "ethereum",
            "ALMANAK_RPC_URL": "https://mainnet.infura.io",
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
        }
        with patch("almanak.framework.execution.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                config = LocalRuntimeConfig.from_env()
        assert config.max_gas_price_gwei == 300

    def test_from_env_berachain_uses_chain_specific_cap(self):
        """Berachain should use its own CHAIN_GAS_PRICE_CAPS_GWEI entry (50 gwei)."""
        env_vars = {
            "ALMANAK_CHAIN": "berachain",
            "ALMANAK_RPC_URL": "https://rpc.berachain.com",
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
        }
        with patch("almanak.framework.execution.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                config = LocalRuntimeConfig.from_env()
        assert config.max_gas_price_gwei == 50  # berachain entry in CHAIN_GAS_PRICE_CAPS_GWEI

    def test_from_env_explicit_override_respected_on_mainnet(self):
        """Explicit ALMANAK_MAX_GAS_PRICE_GWEI should override chain-specific default on mainnet."""
        env_vars = {
            "ALMANAK_CHAIN": "polygon",
            "ALMANAK_RPC_URL": "https://polygon-rpc.com",
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
            "ALMANAK_MAX_GAS_PRICE_GWEI": "1000",
        }
        with patch("almanak.framework.execution.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                config = LocalRuntimeConfig.from_env()
        assert config.max_gas_price_gwei == 1000

    # VIB-304: Anvil mode disables effective gas cap
    def test_from_env_anvil_mode_uses_9999_gwei(self):
        """Anvil mode should use 9999 gwei by default to avoid cap errors during dev."""
        env_vars = {
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
        }
        with patch("almanak.framework.execution.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                config = LocalRuntimeConfig.from_env(chain="arbitrum", network="anvil")
        assert config.max_gas_price_gwei == 9999

    def test_from_env_anvil_mode_ignores_low_override(self):
        """VIB-1719: In Anvil mode, low gas cap from env is overridden to 9999.

        Gas costs nothing on Anvil, so low caps only cause false positives
        (especially on high-gas chains like Polygon).
        """
        env_vars = {
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
            "ALMANAK_MAX_GAS_PRICE_GWEI": "100",
        }
        with patch("almanak.framework.execution.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                config = LocalRuntimeConfig.from_env(chain="polygon", network="anvil")
        assert config.max_gas_price_gwei == 9999

    def test_from_env_anvil_mode_warns_on_low_override(self):
        """VIB-1719: Should warn when user sets a gas cap too low for Anvil mode."""
        env_vars = {
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
            "ALMANAK_MAX_GAS_PRICE_GWEI": "100",
        }
        with patch("almanak.framework.execution.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                with patch("almanak.framework.execution.config.logger") as mock_logger:
                    LocalRuntimeConfig.from_env(chain="polygon", network="anvil")
                mock_logger.warning.assert_called()
                warning_messages = [str(call) for call in mock_logger.warning.call_args_list]
                assert any("too low for Anvil" in msg for msg in warning_messages)

    # VIB-308: Warning for unprefixed env vars
    def test_from_env_warns_on_unprefixed_max_gas_price_gwei(self):
        """Should log a warning when MAX_GAS_PRICE_GWEI is set without ALMANAK_ prefix."""
        env_vars = {
            "ALMANAK_CHAIN": "arbitrum",
            "ALMANAK_RPC_URL": "https://arb1.arbitrum.io/rpc",
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
            "MAX_GAS_PRICE_GWEI": "500",  # missing ALMANAK_ prefix
        }
        with patch("almanak.framework.execution.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                with patch("almanak.framework.execution.config.logger") as mock_logger:
                    LocalRuntimeConfig.from_env()
                # Check that a warning was emitted mentioning MAX_GAS_PRICE_GWEI
                warning_calls = mock_logger.warning.call_args_list
                warning_messages = [str(call) for call in warning_calls]
                assert any("MAX_GAS_PRICE_GWEI" in msg for msg in warning_messages)

    def test_from_env_no_warning_when_prefixed_var_set(self):
        """Should NOT warn when ALMANAK_MAX_GAS_PRICE_GWEI is correctly set."""
        env_vars = {
            "ALMANAK_CHAIN": "arbitrum",
            "ALMANAK_RPC_URL": "https://arb1.arbitrum.io/rpc",
            "ALMANAK_PRIVATE_KEY": TEST_PRIVATE_KEY,
            "ALMANAK_MAX_GAS_PRICE_GWEI": "50",  # correctly prefixed
        }
        with patch("almanak.framework.execution.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                with patch("almanak.framework.execution.config.logger") as mock_logger:
                    LocalRuntimeConfig.from_env()
                # No warning about MAX_GAS_PRICE_GWEI should be emitted
                warning_calls = mock_logger.warning.call_args_list
                warning_messages = [str(call) for call in warning_calls]
                assert not any("MAX_GAS_PRICE_GWEI" in msg and "ignored" in msg for msg in warning_messages)


class TestSecurityContract:
    """Tests for security requirements."""

    def test_private_key_not_in_repr(self):
        """Test that private key is not in repr output."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=TEST_PRIVATE_KEY,
        )
        repr_output = repr(config)
        assert TEST_PRIVATE_KEY not in repr_output
        assert "private_key" not in repr_output.lower()
        # Ensure wallet address IS shown (for debugging)
        assert config.wallet_address in repr_output

    def test_private_key_not_in_str(self):
        """Test that private key is not in str output."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=TEST_PRIVATE_KEY,
        )
        str_output = str(config)
        assert TEST_PRIVATE_KEY not in str_output
        assert "private_key" not in str_output.lower()

    def test_private_key_not_in_to_dict(self):
        """Test that private key is not in to_dict output."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=TEST_PRIVATE_KEY,
        )
        dict_output = config.to_dict()
        assert "private_key" not in dict_output
        assert TEST_PRIVATE_KEY not in str(dict_output)

    def test_rpc_url_masked_in_to_dict(self):
        """Test that API keys in RPC URL are masked in to_dict."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc?api_key=secret123",
            private_key=TEST_PRIVATE_KEY,
        )
        dict_output = config.to_dict()
        assert "secret123" not in dict_output["rpc_url"]
        assert "***" in dict_output["rpc_url"]


class TestSerialization:
    """Tests for serialization and deserialization."""

    def test_to_dict_contains_all_public_fields(self):
        """Test that to_dict contains all public fields."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=TEST_PRIVATE_KEY,
        )
        dict_output = config.to_dict()

        assert "chain" in dict_output
        assert "chain_id" in dict_output
        assert "wallet_address" in dict_output
        assert "max_gas_price_gwei" in dict_output
        assert "tx_timeout_seconds" in dict_output
        assert "simulation_enabled" in dict_output
        assert "created_at" in dict_output

    def test_from_dict_creates_valid_config(self):
        """Test that from_dict creates valid configuration."""
        data = {
            "chain": "optimism",
            "rpc_url": "https://optimism-rpc.com",
            "private_key": TEST_PRIVATE_KEY,
            "max_gas_price_gwei": 75,
            "simulation_enabled": True,
        }
        config = LocalRuntimeConfig.from_dict(data)

        assert config.chain == "optimism"
        assert config.wallet_address == TEST_WALLET_ADDRESS
        assert config.max_gas_price_gwei == 75
        assert config.simulation_enabled is True

    def test_from_dict_missing_private_key_raises_error(self):
        """Test that from_dict without private_key raises error."""
        data = {
            "chain": "arbitrum",
            "rpc_url": "https://arb1.arbitrum.io/rpc",
        }
        with pytest.raises(ConfigurationError) as exc_info:
            LocalRuntimeConfig.from_dict(data)
        assert exc_info.value.field == "private_key"


class TestDerivedProperties:
    """Tests for derived properties."""

    def test_max_gas_price_wei(self):
        """Test max_gas_price_wei property."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=TEST_PRIVATE_KEY,
            max_gas_price_gwei=100,
        )
        assert config.max_gas_price_wei == 100 * 10**9

    def test_max_tx_value_wei(self):
        """Test max_tx_value_wei property."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=TEST_PRIVATE_KEY,
            max_tx_value_eth=10.0,
        )
        assert config.max_tx_value_wei == 10 * 10**18

    def test_get_chain_enum(self):
        """Test get_chain_enum method."""
        config = LocalRuntimeConfig(
            chain="arbitrum",
            rpc_url="https://arb1.arbitrum.io/rpc",
            private_key=TEST_PRIVATE_KEY,
        )
        from almanak.framework.execution.interfaces import Chain

        assert config.get_chain_enum() == Chain.ARBITRUM

    def test_chain_id_derived_correctly(self):
        """Test that chain_id is derived from chain name."""
        for chain_name, expected_id in CHAIN_IDS.items():
            config = LocalRuntimeConfig(
                chain=chain_name,
                rpc_url="https://rpc.example.com",
                private_key=TEST_PRIVATE_KEY,
            )
            assert config.chain_id == expected_id


class TestURLMasking:
    """Tests for URL masking functionality."""

    def test_mask_api_key_in_query(self):
        """Test that API keys in query parameters are masked."""
        url = "https://api.example.com/v1?api_key=secret123&other=value"
        masked = LocalRuntimeConfig._mask_url(url)
        assert "secret123" not in masked
        assert "api_key=***" in masked
        assert "other=value" in masked

    def test_mask_credentials_in_url(self):
        """Test that credentials in URL are masked."""
        url = "https://user:password123@api.example.com/v1"
        masked = LocalRuntimeConfig._mask_url(url)
        assert "password123" not in masked
        assert "user:***@" in masked

    def test_mask_handles_empty_url(self):
        """Test that masking handles empty URL."""
        assert LocalRuntimeConfig._mask_url("") == ""
        assert LocalRuntimeConfig._mask_url(None) is None
