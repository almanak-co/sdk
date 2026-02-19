"""Tests for VaultDepositIntent and VaultRedeemIntent."""

from datetime import datetime
from decimal import Decimal

import pytest

from almanak.framework.intents.vocabulary import (
    Intent,
    IntentType,
    VaultDepositIntent,
    VaultRedeemIntent,
)

VAULT_ADDR = "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB"


# =============================================================================
# VaultDepositIntent
# =============================================================================


class TestVaultDepositIntent:
    def test_create_basic(self):
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("1000"),
        )
        assert intent.protocol == "metamorpho"
        assert intent.vault_address == VAULT_ADDR
        assert intent.amount == Decimal("1000")
        assert intent.intent_type == IntentType.VAULT_DEPOSIT
        assert intent.chain is None

    def test_create_with_chain(self):
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("500"),
            chain="ethereum",
        )
        assert intent.chain == "ethereum"

    def test_create_with_all_amount(self):
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount="all",
        )
        assert intent.amount == "all"
        assert intent.is_chained_amount is True

    def test_is_chained_amount_decimal(self):
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("100"),
        )
        assert intent.is_chained_amount is False

    def test_invalid_zero_amount(self):
        with pytest.raises(ValueError, match="positive"):
            VaultDepositIntent(
                protocol="metamorpho",
                vault_address=VAULT_ADDR,
                amount=Decimal("0"),
            )

    def test_invalid_negative_amount(self):
        with pytest.raises(ValueError, match="positive"):
            VaultDepositIntent(
                protocol="metamorpho",
                vault_address=VAULT_ADDR,
                amount=Decimal("-100"),
            )

    def test_invalid_vault_address(self):
        with pytest.raises(ValueError, match="Invalid vault_address"):
            VaultDepositIntent(
                protocol="metamorpho",
                vault_address="invalid",
                amount=Decimal("1000"),
            )

    def test_invalid_protocol(self):
        with pytest.raises(ValueError, match="Must be 'metamorpho'"):
            VaultDepositIntent(
                protocol="aave_v3",
                vault_address=VAULT_ADDR,
                amount=Decimal("1000"),
            )

    def test_serialize(self):
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("1000"),
            chain="ethereum",
        )
        data = intent.serialize()
        assert data["type"] == "VAULT_DEPOSIT"
        assert data["protocol"] == "metamorpho"
        assert data["vault_address"] == VAULT_ADDR
        assert data["chain"] == "ethereum"

    def test_serialize_all_amount(self):
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount="all",
        )
        data = intent.serialize()
        assert data["amount"] == "all"

    def test_deserialize(self):
        data = {
            "type": "VAULT_DEPOSIT",
            "protocol": "metamorpho",
            "vault_address": VAULT_ADDR,
            "amount": "1000",
            "chain": "ethereum",
        }
        intent = VaultDepositIntent.deserialize(data)
        assert intent.protocol == "metamorpho"
        assert intent.amount == Decimal("1000")
        assert intent.chain == "ethereum"

    def test_roundtrip(self):
        original = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("1234.56"),
            chain="base",
        )
        data = original.serialize()
        restored = VaultDepositIntent.deserialize(data)
        assert restored.protocol == original.protocol
        assert restored.vault_address == original.vault_address
        assert restored.chain == original.chain

    def test_has_intent_id(self):
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("100"),
        )
        assert intent.intent_id is not None
        assert len(intent.intent_id) > 0

    def test_has_created_at(self):
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("100"),
        )
        assert isinstance(intent.created_at, datetime)


# =============================================================================
# VaultRedeemIntent
# =============================================================================


class TestVaultRedeemIntent:
    def test_create_basic(self):
        intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            shares=Decimal("500"),
        )
        assert intent.protocol == "metamorpho"
        assert intent.shares == Decimal("500")
        assert intent.intent_type == IntentType.VAULT_REDEEM

    def test_create_with_all_shares(self):
        intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            shares="all",
        )
        assert intent.shares == "all"
        assert intent.is_chained_amount is True

    def test_is_chained_amount_decimal(self):
        intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            shares=Decimal("100"),
        )
        assert intent.is_chained_amount is False

    def test_invalid_zero_shares(self):
        with pytest.raises(ValueError, match="positive"):
            VaultRedeemIntent(
                protocol="metamorpho",
                vault_address=VAULT_ADDR,
                shares=Decimal("0"),
            )

    def test_invalid_vault_address(self):
        with pytest.raises(ValueError, match="Invalid vault_address"):
            VaultRedeemIntent(
                protocol="metamorpho",
                vault_address="bad",
                shares=Decimal("100"),
            )

    def test_invalid_protocol(self):
        with pytest.raises(ValueError, match="Must be 'metamorpho'"):
            VaultRedeemIntent(
                protocol="compound",
                vault_address=VAULT_ADDR,
                shares=Decimal("100"),
            )

    def test_serialize(self):
        intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            shares=Decimal("500"),
            chain="ethereum",
        )
        data = intent.serialize()
        assert data["type"] == "VAULT_REDEEM"
        assert data["protocol"] == "metamorpho"

    def test_serialize_all_shares(self):
        intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            shares="all",
        )
        data = intent.serialize()
        assert data["shares"] == "all"

    def test_deserialize(self):
        data = {
            "type": "VAULT_REDEEM",
            "protocol": "metamorpho",
            "vault_address": VAULT_ADDR,
            "shares": "500",
            "chain": "ethereum",
        }
        intent = VaultRedeemIntent.deserialize(data)
        assert intent.shares == Decimal("500")

    def test_roundtrip(self):
        original = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            shares=Decimal("100"),
            chain="base",
        )
        data = original.serialize()
        restored = VaultRedeemIntent.deserialize(data)
        assert restored.vault_address == original.vault_address
        assert restored.chain == original.chain


# =============================================================================
# Intent Factory Methods
# =============================================================================


class TestIntentFactory:
    def test_vault_deposit_factory(self):
        intent = Intent.vault_deposit(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("1000"),
            chain="ethereum",
        )
        assert isinstance(intent, VaultDepositIntent)
        assert intent.intent_type == IntentType.VAULT_DEPOSIT

    def test_vault_redeem_factory(self):
        intent = Intent.vault_redeem(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            shares=Decimal("500"),
            chain="ethereum",
        )
        assert isinstance(intent, VaultRedeemIntent)
        assert intent.intent_type == IntentType.VAULT_REDEEM

    def test_vault_redeem_factory_all(self):
        intent = Intent.vault_redeem(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            shares="all",
        )
        assert intent.shares == "all"


# =============================================================================
# Intent.deserialize()
# =============================================================================


class TestIntentDeserialize:
    def test_deserialize_vault_deposit(self):
        data = {
            "type": "VAULT_DEPOSIT",
            "protocol": "metamorpho",
            "vault_address": VAULT_ADDR,
            "amount": "1000",
        }
        intent = Intent.deserialize(data)
        assert isinstance(intent, VaultDepositIntent)
        assert intent.amount == Decimal("1000")

    def test_deserialize_vault_redeem(self):
        data = {
            "type": "VAULT_REDEEM",
            "protocol": "metamorpho",
            "vault_address": VAULT_ADDR,
            "shares": "all",
        }
        intent = Intent.deserialize(data)
        assert isinstance(intent, VaultRedeemIntent)
        assert intent.shares == "all"


# =============================================================================
# IntentType Enum
# =============================================================================


class TestIntentType:
    def test_vault_deposit_exists(self):
        assert IntentType.VAULT_DEPOSIT.value == "VAULT_DEPOSIT"

    def test_vault_redeem_exists(self):
        assert IntentType.VAULT_REDEEM.value == "VAULT_REDEEM"

    def test_vault_reallocate_exists(self):
        assert IntentType.VAULT_REALLOCATE.value == "VAULT_REALLOCATE"

    def test_vault_manage_exists(self):
        assert IntentType.VAULT_MANAGE.value == "VAULT_MANAGE"
