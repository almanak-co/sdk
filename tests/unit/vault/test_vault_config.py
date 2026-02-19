"""Tests for vault configuration and state types."""

from datetime import datetime

import pytest
from web3 import Web3

from almanak.core.models.config import VaultVersion
from almanak.framework.vault.config import (
    SettlementPhase,
    SettlementResult,
    VaultAction,
    VaultConfig,
    VaultState,
)


class TestVaultVersion:
    def test_v0_5_0_exists(self):
        assert VaultVersion.V0_5_0 == "0.5.0"

    def test_v0_5_0_from_string(self):
        assert VaultVersion("0.5.0") == VaultVersion.V0_5_0

    def test_v0_5_0_ordering_in_enum(self):
        versions = list(VaultVersion)
        v030_idx = versions.index(VaultVersion.V0_3_0)
        v050_idx = versions.index(VaultVersion.V0_5_0)
        v100_idx = versions.index(VaultVersion.V1_0_0)
        assert v030_idx < v050_idx < v100_idx


class TestVaultConfig:
    def test_minimal_config(self):
        config = VaultConfig(
            vault_address="0x1234567890abcdef1234567890abcdef12345678",
            valuator_address="0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            underlying_token="USDC",
        )
        assert config.vault_address == Web3.to_checksum_address("0x1234567890abcdef1234567890abcdef12345678")
        assert config.valuator_address == Web3.to_checksum_address("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd")
        assert config.underlying_token == "USDC"

    def test_defaults(self):
        config = VaultConfig(
            vault_address="0x1234567890abcdef1234567890abcdef12345678",
            valuator_address="0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            underlying_token="USDC",
        )
        assert config.version == VaultVersion.V0_5_0
        assert config.settlement_interval_minutes == 60
        assert config.min_valuation_change_down_bps == 500
        assert config.max_valuation_change_up_bps == 1000
        assert config.auto_settle_redeems is True

    def test_custom_values(self):
        config = VaultConfig(
            vault_address="0x1234567890abcdef1234567890abcdef12345678",
            valuator_address="0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            underlying_token="WETH",
            version=VaultVersion.V0_3_0,
            settlement_interval_minutes=120,
            min_valuation_change_down_bps=300,
            max_valuation_change_up_bps=2000,
            auto_settle_redeems=False,
        )
        assert config.underlying_token == "WETH"
        assert config.version == VaultVersion.V0_3_0
        assert config.settlement_interval_minutes == 120
        assert config.min_valuation_change_down_bps == 300
        assert config.max_valuation_change_up_bps == 2000
        assert config.auto_settle_redeems is False

    def test_version_from_string(self):
        config = VaultConfig(
            vault_address="0x1234567890abcdef1234567890abcdef12345678",
            valuator_address="0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            underlying_token="USDC",
            version="0.5.0",
        )
        assert config.version == VaultVersion.V0_5_0

    def test_from_dict(self):
        data = {
            "vault_address": "0x1234567890abcdef1234567890abcdef12345678",
            "valuator_address": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "underlying_token": "USDC",
            "version": "0.5.0",
            "settlement_interval_minutes": 30,
        }
        config = VaultConfig(**data)
        assert config.version == VaultVersion.V0_5_0
        assert config.settlement_interval_minutes == 30

    def test_invalid_vault_address_rejected(self):
        with pytest.raises(Exception, match="Invalid Ethereum address"):
            VaultConfig(
                vault_address="not-an-address",
                valuator_address="0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                underlying_token="USDC",
            )

    def test_short_address_rejected(self):
        with pytest.raises(Exception, match="Invalid Ethereum address"):
            VaultConfig(
                vault_address="0x1234",
                valuator_address="0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                underlying_token="USDC",
            )

    def test_lowercase_address_normalized_to_checksum(self):
        addr = "0x1234567890abcdef1234567890abcdef12345678"
        config = VaultConfig(vault_address=addr, valuator_address=addr, underlying_token="USDC")
        assert config.vault_address == Web3.to_checksum_address(addr)
        assert config.vault_address != addr  # lowercase != checksummed

    def test_mixed_case_address_normalized(self):
        checksummed = Web3.to_checksum_address("0x1234567890abcdef1234567890abcdef12345678")
        config = VaultConfig(vault_address=checksummed, valuator_address=checksummed, underlying_token="USDC")
        assert config.vault_address == checksummed

    def test_uppercase_address_normalized(self):
        upper = "0x" + "1234567890ABCDEF1234567890ABCDEF12345678"
        config = VaultConfig(vault_address=upper, valuator_address=upper, underlying_token="USDC")
        assert config.vault_address == Web3.to_checksum_address(upper)


class TestVaultState:
    def test_defaults(self):
        state = VaultState()
        assert state.last_valuation_time is None
        assert state.last_total_assets == 0
        assert state.last_proposed_total_assets == 0
        assert state.last_pending_deposits == 0
        assert state.last_settlement_epoch == 0
        assert state.settlement_phase == SettlementPhase.IDLE
        assert state.initialized is False

    def test_custom_values(self):
        now = datetime(2026, 2, 15, 12, 0, 0)
        state = VaultState(
            last_valuation_time=now,
            last_total_assets=1_000_000,
            last_proposed_total_assets=1_010_000,
            last_pending_deposits=50_000,
            last_settlement_epoch=5,
            settlement_phase=SettlementPhase.PROPOSED,
            initialized=True,
        )
        assert state.last_valuation_time == now
        assert state.last_total_assets == 1_000_000
        assert state.last_proposed_total_assets == 1_010_000
        assert state.last_pending_deposits == 50_000
        assert state.last_settlement_epoch == 5
        assert state.settlement_phase == SettlementPhase.PROPOSED
        assert state.initialized is True


class TestSettlementPhase:
    def test_all_phases(self):
        assert SettlementPhase.IDLE.value == "idle"
        assert SettlementPhase.PROPOSING.value == "proposing"
        assert SettlementPhase.PROPOSED.value == "proposed"
        assert SettlementPhase.SETTLING.value == "settling"
        assert SettlementPhase.SETTLED.value == "settled"

    def test_phase_count(self):
        assert len(SettlementPhase) == 5


class TestVaultAction:
    def test_all_actions(self):
        assert VaultAction.HOLD.value == "hold"
        assert VaultAction.SETTLE.value == "settle"
        assert VaultAction.RESUME_SETTLE.value == "resume_settle"


class TestSettlementResult:
    def test_defaults(self):
        result = SettlementResult(success=True)
        assert result.success is True
        assert result.deposits_received == 0
        assert result.redemptions_processed == 0
        assert result.new_total_assets == 0
        assert result.shares_minted == 0
        assert result.shares_burned == 0
        assert result.fee_shares_minted == 0
        assert result.epoch_id == 0

    def test_full_result(self):
        result = SettlementResult(
            success=True,
            deposits_received=100_000,
            redemptions_processed=20_000,
            new_total_assets=1_080_000,
            shares_minted=99_000,
            shares_burned=19_800,
            fee_shares_minted=200,
            epoch_id=6,
        )
        assert result.deposits_received == 100_000
        assert result.redemptions_processed == 20_000
        assert result.new_total_assets == 1_080_000
        assert result.shares_minted == 99_000
        assert result.shares_burned == 19_800
        assert result.fee_shares_minted == 200
        assert result.epoch_id == 6

    def test_failed_result(self):
        result = SettlementResult(success=False)
        assert result.success is False
