"""Tests for LocalKeySigner.

This test suite covers:
- Initialization with valid/invalid private keys
- EIP-1559 transaction signing
- Legacy transaction signing
- Transaction validation (nonce, from_address, gas fields, data format)
- Security contract (no key exposure)
- Error handling
"""

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

import pytest

from almanak.framework.execution.interfaces import (
    SignedTransaction,
    SigningError,
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.signer.local import LocalKeySigner

T = TypeVar("T")


def run_async[T](coro: Coroutine[Any, Any, T]) -> T:
    """Helper to run async functions in sync tests."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# =============================================================================
# Test Private Keys (DO NOT use these in production!)
# =============================================================================

# Well-known test private key (Ganache default account 0)
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_ADDRESS = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Second test key for validation tests
TEST_PRIVATE_KEY_2 = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
TEST_ADDRESS_2 = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"


# =============================================================================
# Initialization Tests
# =============================================================================


class TestLocalKeySignerInit:
    """Tests for LocalKeySigner initialization."""

    def test_init_with_valid_private_key(self) -> None:
        """Test initialization with a valid private key."""
        signer = LocalKeySigner(private_key=TEST_PRIVATE_KEY)

        assert signer.address == TEST_ADDRESS

    def test_init_with_private_key_no_0x_prefix(self) -> None:
        """Test initialization with private key without 0x prefix."""
        # Remove 0x prefix
        key_without_prefix = TEST_PRIVATE_KEY[2:]
        signer = LocalKeySigner(private_key=key_without_prefix)

        assert signer.address == TEST_ADDRESS

    def test_init_with_invalid_private_key(self) -> None:
        """Test initialization with invalid private key raises SigningError."""
        with pytest.raises(SigningError) as exc_info:
            LocalKeySigner(private_key="not_a_valid_key")

        assert "Invalid private key format" in str(exc_info.value)
        # Verify the original key is NOT in the error message
        assert "not_a_valid_key" not in str(exc_info.value)

    def test_init_with_empty_key(self) -> None:
        """Test initialization with empty key raises SigningError."""
        with pytest.raises(SigningError) as exc_info:
            LocalKeySigner(private_key="")

        assert "Invalid private key format" in str(exc_info.value)

    def test_init_with_short_key(self) -> None:
        """Test initialization with key that's too short."""
        with pytest.raises(SigningError) as exc_info:
            LocalKeySigner(private_key="0x1234")

        assert "Invalid private key format" in str(exc_info.value)

    def test_address_is_checksummed(self) -> None:
        """Test that the returned address is properly checksummed."""
        signer = LocalKeySigner(private_key=TEST_PRIVATE_KEY)

        # Check that the address has mixed case (indicates checksum)
        assert signer.address.startswith("0x")
        assert len(signer.address) == 42
        # The test address has mixed case due to checksum
        assert any(c.isupper() for c in signer.address[2:])
        assert any(c.islower() for c in signer.address[2:])


# =============================================================================
# EIP-1559 Transaction Signing Tests
# =============================================================================


class TestEIP1559Signing:
    """Tests for EIP-1559 (Type 2) transaction signing."""

    @pytest.fixture
    def signer(self) -> LocalKeySigner:
        """Create a signer for tests."""
        return LocalKeySigner(private_key=TEST_PRIVATE_KEY)

    @pytest.fixture
    def eip1559_tx(self) -> UnsignedTransaction:
        """Create a valid EIP-1559 transaction."""
        return UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=1_000_000_000_000_000_000,  # 1 ETH
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            max_fee_per_gas=30_000_000_000,  # 30 gwei
            max_priority_fee_per_gas=1_000_000_000,  # 1 gwei
            tx_type=TransactionType.EIP_1559,
        )

    def test_sign_eip1559_transaction(self, signer: LocalKeySigner, eip1559_tx: UnsignedTransaction) -> None:
        """Test signing a valid EIP-1559 transaction."""
        signed = run_async(signer.sign(eip1559_tx, chain="ethereum"))

        assert isinstance(signed, SignedTransaction)
        assert signed.raw_tx.startswith("0x")
        assert signed.tx_hash.startswith("0x")
        assert len(signed.tx_hash) == 66  # 0x + 64 hex chars
        assert signed.unsigned_tx == eip1559_tx
        assert signed.signed_at is not None

    def test_sign_eip1559_with_calldata(self, signer: LocalKeySigner) -> None:
        """Test signing EIP-1559 transaction with calldata."""
        # ERC-20 transfer calldata
        calldata = (
            "0xa9059cbb"
            "000000000000000000000000"
            "70997970c51812dc3a010c7d01b50e0d17dc79c8"
            "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
        )

        tx = UnsignedTransaction(
            to="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC contract
            value=0,
            data=calldata,
            chain_id=1,
            gas_limit=100000,
            nonce=5,
            max_fee_per_gas=50_000_000_000,
            max_priority_fee_per_gas=2_000_000_000,
        )

        signed = run_async(signer.sign(tx, chain="ethereum"))

        assert signed.raw_tx.startswith("0x")
        assert signed.tx_hash.startswith("0x")

    def test_sign_contract_creation(self, signer: LocalKeySigner) -> None:
        """Test signing a contract creation transaction (to=None)."""
        tx = UnsignedTransaction(
            to=None,  # Contract creation
            value=0,
            data="0x6080604052",  # Simple bytecode
            chain_id=1,
            gas_limit=500000,
            nonce=0,
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
        )

        signed = run_async(signer.sign(tx, chain="ethereum"))

        assert signed.raw_tx.startswith("0x")
        assert signed.tx_hash.startswith("0x")

    def test_sign_arbitrum_transaction(self, signer: LocalKeySigner) -> None:
        """Test signing transaction for Arbitrum chain."""
        tx = UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=0,
            data="0x",
            chain_id=42161,  # Arbitrum One
            gas_limit=100000,
            nonce=0,
            max_fee_per_gas=100_000_000,  # 0.1 gwei
            max_priority_fee_per_gas=1_000_000,
        )

        signed = run_async(signer.sign(tx, chain="arbitrum"))

        assert signed.raw_tx.startswith("0x")
        assert signed.tx_hash.startswith("0x")


# =============================================================================
# Legacy Transaction Signing Tests
# =============================================================================


class TestLegacyTransactionSigning:
    """Tests for legacy (Type 0) transaction signing."""

    @pytest.fixture
    def signer(self) -> LocalKeySigner:
        """Create a signer for tests."""
        return LocalKeySigner(private_key=TEST_PRIVATE_KEY)

    @pytest.fixture
    def legacy_tx(self) -> UnsignedTransaction:
        """Create a valid legacy transaction."""
        return UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=1_000_000_000_000_000_000,  # 1 ETH
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            gas_price=20_000_000_000,  # 20 gwei
            tx_type=TransactionType.LEGACY,
        )

    def test_sign_legacy_transaction(self, signer: LocalKeySigner, legacy_tx: UnsignedTransaction) -> None:
        """Test signing a valid legacy transaction."""
        signed = run_async(signer.sign(legacy_tx, chain="ethereum"))

        assert isinstance(signed, SignedTransaction)
        assert signed.raw_tx.startswith("0x")
        assert signed.tx_hash.startswith("0x")
        assert len(signed.tx_hash) == 66
        assert signed.unsigned_tx == legacy_tx

    def test_sign_legacy_with_calldata(self, signer: LocalKeySigner) -> None:
        """Test signing legacy transaction with calldata."""
        tx = UnsignedTransaction(
            to="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            value=0,
            data="0xa9059cbb" + "0" * 128,  # Simplified transfer calldata
            chain_id=1,
            gas_limit=100000,
            nonce=10,
            gas_price=30_000_000_000,
            tx_type=TransactionType.LEGACY,
        )

        signed = run_async(signer.sign(tx, chain="ethereum"))

        assert signed.raw_tx.startswith("0x")


# =============================================================================
# Validation Tests
# =============================================================================


class TestTransactionValidation:
    """Tests for transaction validation before signing."""

    @pytest.fixture
    def signer(self) -> LocalKeySigner:
        """Create a signer for tests."""
        return LocalKeySigner(private_key=TEST_PRIVATE_KEY)

    def test_validation_nonce_required(self, signer: LocalKeySigner) -> None:
        """Test that nonce must be set before signing."""
        tx = UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=0,
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=None,  # Not set
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
        )

        with pytest.raises(SigningError) as exc_info:
            run_async(signer.sign(tx, chain="ethereum"))

        assert "nonce must be set" in str(exc_info.value)

    def test_validation_from_address_mismatch(self, signer: LocalKeySigner) -> None:
        """Test that from_address must match signer address if set."""
        tx = UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=0,
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
            from_address=TEST_ADDRESS_2,  # Different from signer
        )

        with pytest.raises(SigningError) as exc_info:
            run_async(signer.sign(tx, chain="ethereum"))

        assert "does not match signer address" in str(exc_info.value)

    def test_validation_from_address_matches(self, signer: LocalKeySigner) -> None:
        """Test that from_address matching signer address passes validation."""
        tx = UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=0,
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
            from_address=TEST_ADDRESS,  # Matches signer
        )

        # Should not raise
        signed = run_async(signer.sign(tx, chain="ethereum"))
        assert signed.tx_hash.startswith("0x")

    def test_validation_priority_fee_exceeds_max_fee(self, signer: LocalKeySigner) -> None:
        """Test that max_priority_fee cannot exceed max_fee."""
        tx = UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=0,
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            max_fee_per_gas=10_000_000_000,  # 10 gwei
            max_priority_fee_per_gas=20_000_000_000,  # 20 gwei (exceeds max_fee)
        )

        with pytest.raises(SigningError) as exc_info:
            run_async(signer.sign(tx, chain="ethereum"))

        assert "cannot exceed max_fee_per_gas" in str(exc_info.value)

    def test_validation_data_must_have_0x_prefix(self, signer: LocalKeySigner) -> None:
        """Test that data must have 0x prefix."""
        tx = UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=0,
            data="a9059cbb",  # Missing 0x prefix
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
        )

        with pytest.raises(SigningError) as exc_info:
            run_async(signer.sign(tx, chain="ethereum"))

        assert "hex-encoded with 0x prefix" in str(exc_info.value)

    def test_validation_invalid_to_address(self, signer: LocalKeySigner) -> None:
        """Test that to address must be valid format."""
        tx = UnsignedTransaction(
            to="invalid_address",
            value=0,
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
        )

        with pytest.raises(SigningError) as exc_info:
            run_async(signer.sign(tx, chain="ethereum"))

        assert "Invalid 'to' address format" in str(exc_info.value)

    def test_validation_to_address_wrong_length(self, signer: LocalKeySigner) -> None:
        """Test that to address must be correct length."""
        tx = UnsignedTransaction(
            to="0x1234",  # Too short
            value=0,
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
        )

        with pytest.raises(SigningError) as exc_info:
            run_async(signer.sign(tx, chain="ethereum"))

        assert "Invalid 'to' address format" in str(exc_info.value)


# =============================================================================
# Security Tests
# =============================================================================


class TestSecurityContract:
    """Tests to verify the security contract is upheld."""

    def test_private_key_not_in_repr(self) -> None:
        """Test that private key is not exposed in repr."""
        signer = LocalKeySigner(private_key=TEST_PRIVATE_KEY)
        repr_str = repr(signer)

        # The key should NOT appear in repr
        assert TEST_PRIVATE_KEY not in repr_str
        assert TEST_PRIVATE_KEY[2:] not in repr_str  # Without 0x prefix
        # The address should appear
        assert TEST_ADDRESS in repr_str or TEST_ADDRESS.lower() in repr_str.lower()

    def test_private_key_not_in_str(self) -> None:
        """Test that private key is not exposed in str."""
        signer = LocalKeySigner(private_key=TEST_PRIVATE_KEY)
        str_repr = str(signer)

        assert TEST_PRIVATE_KEY not in str_repr
        assert TEST_PRIVATE_KEY[2:] not in str_repr

    def test_private_key_not_in_error_message(self) -> None:
        """Test that invalid private key is not included in error."""
        # This is clearly invalid - not a valid hex string
        bad_key = "0xthis_is_not_valid_hex_and_will_fail"

        with pytest.raises(SigningError) as exc_info:
            LocalKeySigner(private_key=bad_key)

        # The bad key should not appear in the error
        error_str = str(exc_info.value)
        assert bad_key not in error_str
        assert bad_key[2:] not in error_str

    def test_no_private_key_attribute_exposed(self) -> None:
        """Test that private key cannot be accessed via attributes."""
        signer = LocalKeySigner(private_key=TEST_PRIVATE_KEY)

        # These attributes should not exist or be accessible
        assert not hasattr(signer, "private_key")
        assert not hasattr(signer, "key")
        assert not hasattr(signer, "_private_key")
        assert not hasattr(signer, "_key")


# =============================================================================
# Batch Signing Tests
# =============================================================================


class TestBatchSigning:
    """Tests for batch signing functionality."""

    @pytest.fixture
    def signer(self) -> LocalKeySigner:
        """Create a signer for tests."""
        return LocalKeySigner(private_key=TEST_PRIVATE_KEY)

    def test_sign_batch_empty_list(self, signer: LocalKeySigner) -> None:
        """Test signing empty batch returns empty list."""
        result = run_async(signer.sign_batch([], chain="ethereum"))

        assert result == []

    def test_sign_batch_single_tx(self, signer: LocalKeySigner) -> None:
        """Test signing batch with single transaction."""
        tx = UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=0,
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
        )

        result = run_async(signer.sign_batch([tx], chain="ethereum"))

        assert len(result) == 1
        assert result[0].tx_hash.startswith("0x")

    def test_sign_batch_multiple_txs(self, signer: LocalKeySigner) -> None:
        """Test signing batch with multiple transactions."""
        txs = [
            UnsignedTransaction(
                to="0x1234567890123456789012345678901234567890",
                value=0,
                data="0x",
                chain_id=1,
                gas_limit=21000,
                nonce=i,
                max_fee_per_gas=30_000_000_000,
                max_priority_fee_per_gas=1_000_000_000,
            )
            for i in range(3)
        ]

        result = run_async(signer.sign_batch(txs, chain="ethereum"))

        assert len(result) == 3
        # All hashes should be unique
        hashes = [r.tx_hash for r in result]
        assert len(set(hashes)) == 3

    def test_sign_batch_preserves_order(self, signer: LocalKeySigner) -> None:
        """Test that batch signing preserves transaction order."""
        txs = [
            UnsignedTransaction(
                to="0x1234567890123456789012345678901234567890",
                value=i * 1_000_000_000_000_000_000,  # Different values
                data="0x",
                chain_id=1,
                gas_limit=21000,
                nonce=i,
                max_fee_per_gas=30_000_000_000,
                max_priority_fee_per_gas=1_000_000_000,
            )
            for i in range(3)
        ]

        result = run_async(signer.sign_batch(txs, chain="ethereum"))

        # Verify order by checking original transactions
        for i, signed in enumerate(result):
            assert signed.unsigned_tx.nonce == i
            assert signed.unsigned_tx.value == i * 1_000_000_000_000_000_000


# =============================================================================
# Transaction Hash Verification Tests
# =============================================================================


class TestTransactionHashVerification:
    """Tests to verify transaction hashes are correct."""

    @pytest.fixture
    def signer(self) -> LocalKeySigner:
        """Create a signer for tests."""
        return LocalKeySigner(private_key=TEST_PRIVATE_KEY)

    def test_same_tx_produces_same_hash(self, signer: LocalKeySigner) -> None:
        """Test that signing the same transaction produces the same hash."""
        tx = UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=1_000_000_000_000_000_000,
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
        )

        signed1 = run_async(signer.sign(tx, chain="ethereum"))
        signed2 = run_async(signer.sign(tx, chain="ethereum"))

        assert signed1.tx_hash == signed2.tx_hash
        assert signed1.raw_tx == signed2.raw_tx

    def test_different_nonce_produces_different_hash(self, signer: LocalKeySigner) -> None:
        """Test that different nonces produce different hashes."""
        tx1 = UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=0,
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
        )

        tx2 = UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=0,
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=1,  # Different nonce
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
        )

        signed1 = run_async(signer.sign(tx1, chain="ethereum"))
        signed2 = run_async(signer.sign(tx2, chain="ethereum"))

        assert signed1.tx_hash != signed2.tx_hash

    def test_different_signer_produces_different_hash(self) -> None:
        """Test that different signers produce different hashes for same tx."""
        signer1 = LocalKeySigner(private_key=TEST_PRIVATE_KEY)
        signer2 = LocalKeySigner(private_key=TEST_PRIVATE_KEY_2)

        tx = UnsignedTransaction(
            to="0x1234567890123456789012345678901234567890",
            value=0,
            data="0x",
            chain_id=1,
            gas_limit=21000,
            nonce=0,
            max_fee_per_gas=30_000_000_000,
            max_priority_fee_per_gas=1_000_000_000,
        )

        signed1 = run_async(signer1.sign(tx, chain="ethereum"))
        signed2 = run_async(signer2.sign(tx, chain="ethereum"))

        # Different signers should produce different signatures and hashes
        assert signed1.tx_hash != signed2.tx_hash
        assert signed1.raw_tx != signed2.raw_tx
