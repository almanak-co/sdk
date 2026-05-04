"""Tests for Kraken models."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import SecretStr

from almanak.framework.connectors.kraken.models import (
    CEXIdempotencyKey,
    CEXOperationType,
    CEXRiskConfig,
    KrakenBalance,
    KrakenConfig,
    KrakenCredentials,
    KrakenDepositStatus,
    KrakenMarketInfo,
    KrakenOrderStatus,
    KrakenWithdrawStatus,
)


class TestKrakenCredentials:
    """Tests for KrakenCredentials model."""

    def test_create_credentials(self):
        """Should create credentials with SecretStr."""
        creds = KrakenCredentials(
            api_key=SecretStr("test_key"),
            api_secret=SecretStr("test_secret"),
        )
        assert creds.api_key.get_secret_value() == "test_key"
        assert creds.api_secret.get_secret_value() == "test_secret"

    def test_credentials_mask_in_repr(self):
        """Credentials should be masked in string representation."""
        creds = KrakenCredentials(
            api_key=SecretStr("test_key"),
            api_secret=SecretStr("test_secret"),
        )
        repr_str = repr(creds)
        assert "test_key" not in repr_str
        assert "test_secret" not in repr_str
        assert "**********" in repr_str

    def test_credentials_from_env(self, monkeypatch):
        """Should load credentials from environment."""
        monkeypatch.setenv("KRAKEN_API_KEY", "env_key")
        monkeypatch.setenv("KRAKEN_API_SECRET", "env_secret")

        creds = KrakenCredentials.from_env()
        assert creds.api_key.get_secret_value() == "env_key"
        assert creds.api_secret.get_secret_value() == "env_secret"

    def test_credentials_from_env_custom_vars(self, monkeypatch):
        """Should load from custom environment variable names."""
        monkeypatch.setenv("MY_KRAKEN_KEY", "custom_key")
        monkeypatch.setenv("MY_KRAKEN_SECRET", "custom_secret")

        creds = KrakenCredentials.from_env(
            key_env="MY_KRAKEN_KEY",
            secret_env="MY_KRAKEN_SECRET",
        )
        assert creds.api_key.get_secret_value() == "custom_key"
        assert creds.api_secret.get_secret_value() == "custom_secret"

    def test_credentials_from_env_missing_key(self, monkeypatch):
        """Should raise error if key not set."""
        monkeypatch.delenv("KRAKEN_API_KEY", raising=False)
        monkeypatch.setenv("KRAKEN_API_SECRET", "secret")

        with pytest.raises(ValueError, match="KRAKEN_API_KEY not set"):
            KrakenCredentials.from_env()

    def test_credentials_from_env_missing_secret(self, monkeypatch):
        """Should raise error if secret not set."""
        monkeypatch.setenv("KRAKEN_API_KEY", "key")
        monkeypatch.delenv("KRAKEN_API_SECRET", raising=False)

        with pytest.raises(ValueError, match="KRAKEN_API_SECRET not set"):
            KrakenCredentials.from_env()

    def test_empty_credentials_rejected(self):
        """Should reject empty credentials."""
        with pytest.raises(ValueError, match="API key cannot be empty"):
            KrakenCredentials(
                api_key=SecretStr(""),
                api_secret=SecretStr("test"),
            )

        with pytest.raises(ValueError, match="API secret cannot be empty"):
            KrakenCredentials(
                api_key=SecretStr("test"),
                api_secret=SecretStr(""),
            )


class TestKrakenConfig:
    """Tests for KrakenConfig model."""

    def test_default_config(self):
        """Should create config with defaults."""
        config = KrakenConfig()
        assert config.default_slippage_bps == 50
        assert config.order_timeout_seconds == 300
        assert config.withdrawal_timeout_seconds == 3600
        assert config.poll_interval_seconds == 2.0
        assert config.require_withdrawal_whitelist is True

    def test_config_with_credentials(self):
        """Should store credentials when provided."""
        creds = KrakenCredentials(
            api_key=SecretStr("key"),
            api_secret=SecretStr("secret"),
        )
        config = KrakenConfig(credentials=creds)
        assert config.credentials == creds
        assert config.get_credentials() == creds

    def test_config_get_credentials_from_env(self, monkeypatch):
        """Should load credentials from env when not provided."""
        monkeypatch.setenv("KRAKEN_API_KEY", "env_key")
        monkeypatch.setenv("KRAKEN_API_SECRET", "env_secret")

        config = KrakenConfig()  # No credentials provided
        creds = config.get_credentials()
        assert creds.api_key.get_secret_value() == "env_key"

    def test_slippage_validation(self):
        """Should validate slippage range."""
        # Valid slippage
        config = KrakenConfig(default_slippage_bps=100)
        assert config.default_slippage_bps == 100

        # Too high
        with pytest.raises(ValueError):
            KrakenConfig(default_slippage_bps=1001)

        # Negative
        with pytest.raises(ValueError):
            KrakenConfig(default_slippage_bps=-1)


class TestCEXRiskConfig:
    """Tests for CEXRiskConfig model."""

    def test_default_risk_config(self):
        """Should create with defaults."""
        config = CEXRiskConfig()
        assert config.max_order_size_usd == Decimal("50000")
        assert config.max_daily_withdrawal_usd == Decimal("100000")
        assert config.max_outstanding_orders == 5
        assert "arbitrum" in config.allowed_withdrawal_chains

    def test_chains_normalized_to_lowercase(self):
        """Should normalize chain names to lowercase."""
        config = CEXRiskConfig(allowed_withdrawal_chains=["ARBITRUM", "Optimism"])
        assert config.allowed_withdrawal_chains == ["arbitrum", "optimism"]


class TestKrakenMarketInfo:
    """Tests for KrakenMarketInfo model."""

    def test_from_kraken_response(self):
        """Should parse Kraken API response."""
        data = {
            "base": "XETH",
            "quote": "ZUSD",
            "pair_decimals": 2,
            "lot_decimals": 8,
            "ordermin": "0.01",
            "costmin": "5.0",
            "fees": [[0, 0.26]],
            "fees_maker": [[0, 0.16]],
        }
        info = KrakenMarketInfo.from_kraken_response("ETHUSD", data)
        assert info.pair == "ETHUSD"
        assert info.base_asset == "XETH"
        assert info.quote_asset == "ZUSD"
        assert info.ordermin == Decimal("0.01")
        assert info.costmin == Decimal("5.0")
        assert info.taker_fee == Decimal("0.26")
        assert info.maker_fee == Decimal("0.16")

    def test_get_min_order_base(self):
        """Should calculate minimum order in wei."""
        info = KrakenMarketInfo(
            pair="ETHUSD",
            base_asset="ETH",
            quote_asset="USD",
            pair_decimals=2,
            lot_decimals=8,
            ordermin=Decimal("0.01"),
            costmin=Decimal("5.0"),
            taker_fee=Decimal("0.26"),
            maker_fee=Decimal("0.16"),
        )
        # 0.01 ETH with 18 decimals = 10000000000000000 wei
        min_wei = info.get_min_order_base(18)
        assert min_wei == 10000000000000000

    def test_get_min_cost_quote(self):
        """Should calculate minimum cost in wei."""
        info = KrakenMarketInfo(
            pair="ETHUSD",
            base_asset="ETH",
            quote_asset="USD",
            pair_decimals=2,
            lot_decimals=8,
            ordermin=Decimal("0.01"),
            costmin=Decimal("5.0"),
            taker_fee=Decimal("0.26"),
            maker_fee=Decimal("0.16"),
        )
        # 5.0 USD with 6 decimals = 5000000
        min_cost = info.get_min_cost_quote(6)
        assert min_cost == 5000000


class TestCEXIdempotencyKey:
    """Tests for CEXIdempotencyKey dataclass."""

    def test_create_swap_key(self):
        """Should create key for swap operation."""
        key = CEXIdempotencyKey(
            action_id="action_1",
            exchange="kraken",
            operation_type=CEXOperationType.SWAP,
            userref=12345678,
        )
        assert key.action_id == "action_1"
        assert key.exchange == "kraken"
        assert key.operation_type == CEXOperationType.SWAP
        assert key.userref == 12345678
        assert key.status == "pending"

    def test_create_withdrawal_key(self):
        """Should create key for withdrawal operation."""
        key = CEXIdempotencyKey(
            action_id="action_2",
            exchange="kraken",
            operation_type=CEXOperationType.WITHDRAW,
            refid="REFID123",
        )
        assert key.operation_type == CEXOperationType.WITHDRAW
        assert key.refid == "REFID123"

    def test_to_dict_roundtrip(self):
        """Should serialize and deserialize correctly."""
        key = CEXIdempotencyKey(
            action_id="action_1",
            exchange="kraken",
            operation_type=CEXOperationType.SWAP,
            userref=12345678,
            order_id="OXXXXXX",
            status="closed",
        )

        data = key.to_dict()
        restored = CEXIdempotencyKey.from_dict(data)

        assert restored.action_id == key.action_id
        assert restored.exchange == key.exchange
        assert restored.operation_type == key.operation_type
        assert restored.userref == key.userref
        assert restored.order_id == key.order_id
        assert restored.status == key.status

    def test_timestamp_serialization(self):
        """Should preserve timestamps in serialization."""
        now = datetime.now(UTC)
        key = CEXIdempotencyKey(
            action_id="test",
            exchange="kraken",
            operation_type=CEXOperationType.SWAP,
            created_at=now,
            last_poll=now,
        )

        data = key.to_dict()
        assert data["created_at"] == now.isoformat()
        assert data["last_poll"] == now.isoformat()

        restored = CEXIdempotencyKey.from_dict(data)
        assert restored.created_at.isoformat() == now.isoformat()
        assert restored.last_poll.isoformat() == now.isoformat()


class TestKrakenBalance:
    """Tests for KrakenBalance model."""

    def test_from_kraken_response(self):
        """Should parse Kraken balance response."""
        data = {"balance": "1.5", "hold_trade": "0.2"}
        balance = KrakenBalance.from_kraken_response("ETH", data)

        assert balance.asset == "ETH"
        assert balance.total == Decimal("1.5")
        assert balance.held == Decimal("0.2")
        assert balance.available == Decimal("1.3")

    def test_from_kraken_response_no_hold(self):
        """Should handle response without hold."""
        data = {"balance": "100.0"}
        balance = KrakenBalance.from_kraken_response("USDC", data)

        assert balance.total == Decimal("100.0")
        assert balance.held == Decimal("0")
        assert balance.available == Decimal("100.0")


class TestEnums:
    """Tests for Kraken enums."""

    def test_order_status_values(self):
        """Should have expected order status values."""
        assert KrakenOrderStatus.PENDING == "pending"
        assert KrakenOrderStatus.OPEN == "open"
        assert KrakenOrderStatus.CLOSED == "closed"
        assert KrakenOrderStatus.CANCELED == "canceled"

    def test_withdraw_status_values(self):
        """Should have expected withdrawal status values."""
        assert KrakenWithdrawStatus.INITIAL == "Initial"
        assert KrakenWithdrawStatus.PENDING == "Pending"
        assert KrakenWithdrawStatus.SUCCESS == "Success"

    def test_deposit_status_values(self):
        """Should have expected deposit status values."""
        assert KrakenDepositStatus.PENDING == "Pending"
        assert KrakenDepositStatus.SUCCESS == "Success"

    def test_operation_type_values(self):
        """Should have expected operation type values."""
        assert CEXOperationType.SWAP == "swap"
        assert CEXOperationType.WITHDRAW == "withdraw"
        assert CEXOperationType.DEPOSIT == "deposit"
