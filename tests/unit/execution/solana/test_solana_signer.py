"""Tests for Solana Ed25519 signer."""

import base64

import pytest
from solders.hash import Hash as SolHash
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.signature import Signature
from solders.transaction import VersionedTransaction

import solders.system_program as sp

from almanak.framework.execution.solana.signer import SolanaSigner, SolanaSignerError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def keypair():
    return Keypair()


@pytest.fixture
def signer(keypair):
    return SolanaSigner(keypair)


@pytest.fixture
def unsigned_tx_base64(keypair):
    """Create a simple unsigned VersionedTransaction as base64."""
    ix = sp.transfer(
        sp.TransferParams(
            from_pubkey=keypair.pubkey(),
            to_pubkey=keypair.pubkey(),
            lamports=1000,
        )
    )
    msg = MessageV0.try_compile(keypair.pubkey(), [ix], [], SolHash.default())
    # Create with null signature
    unsigned_tx = VersionedTransaction.populate(msg, [Signature.default()])
    return base64.b64encode(bytes(unsigned_tx)).decode()


# ---------------------------------------------------------------------------
# Construction tests
# ---------------------------------------------------------------------------


class TestSolanaSignerConstruction:
    def test_from_keypair(self, keypair):
        signer = SolanaSigner(keypair)
        assert signer.pubkey == keypair.pubkey()

    def test_from_base58(self, keypair):
        b58 = str(keypair)
        signer = SolanaSigner.from_base58(b58)
        assert signer.pubkey == keypair.pubkey()

    def test_from_bytes(self, keypair):
        key_bytes = bytes(keypair)
        signer = SolanaSigner.from_bytes(key_bytes)
        assert signer.pubkey == keypair.pubkey()

    def test_wallet_address(self, signer, keypair):
        assert signer.wallet_address == str(keypair.pubkey())


# ---------------------------------------------------------------------------
# Signing tests
# ---------------------------------------------------------------------------


class TestSolanaSignerSigning:
    def test_sign_serialized_transaction(self, signer, unsigned_tx_base64):
        signed_b64 = signer.sign_serialized_transaction(unsigned_tx_base64)

        # Decode and verify
        signed_bytes = base64.b64decode(signed_b64)
        signed_tx = VersionedTransaction.from_bytes(signed_bytes)

        assert len(signed_tx.signatures) == 1
        # Signature should not be all zeros
        assert signed_tx.signatures[0] != Signature.default()

    def test_signed_tx_is_valid_base64(self, signer, unsigned_tx_base64):
        signed_b64 = signer.sign_serialized_transaction(unsigned_tx_base64)
        # Should decode without error
        decoded = base64.b64decode(signed_b64)
        assert len(decoded) > 0

    def test_sign_invalid_base64_raises(self, signer):
        with pytest.raises(SolanaSignerError, match="Failed to sign"):
            signer.sign_serialized_transaction("not-valid-base64!!!")

    def test_sign_corrupted_tx_raises(self, signer):
        # Valid base64 but not a valid transaction
        corrupt_b64 = base64.b64encode(b"garbage-data-not-a-transaction").decode()
        with pytest.raises(SolanaSignerError, match="Failed to sign"):
            signer.sign_serialized_transaction(corrupt_b64)


# ---------------------------------------------------------------------------
# Message signing tests
# ---------------------------------------------------------------------------


class TestSolanaSignerMultiSigner:
    """Tests for multi-signer transaction support (e.g., Raydium LP open)."""

    def test_sign_with_additional_signer(self, keypair):
        """Sign a 2-signer transaction (wallet + additional keypair)."""
        signer = SolanaSigner(keypair)
        additional_kp = Keypair()

        # Build a tx that requires 2 signers:
        # ix1: wallet transfers to additional_kp (wallet signs)
        # ix2: additional_kp transfers back (additional_kp signs)
        ix1 = sp.transfer(
            sp.TransferParams(
                from_pubkey=keypair.pubkey(),
                to_pubkey=additional_kp.pubkey(),
                lamports=1000,
            )
        )
        ix2 = sp.transfer(
            sp.TransferParams(
                from_pubkey=additional_kp.pubkey(),
                to_pubkey=keypair.pubkey(),
                lamports=500,
            )
        )
        msg = MessageV0.try_compile(keypair.pubkey(), [ix1, ix2], [], SolHash.default())
        assert msg.header.num_required_signatures == 2  # Confirm 2 signers needed

        unsigned_tx = VersionedTransaction.populate(
            msg, [Signature.default()] * msg.header.num_required_signatures
        )
        tx_b64 = base64.b64encode(bytes(unsigned_tx)).decode()

        # Encode additional signer as base64 bytes (same format as Raydium adapter)
        additional_signer_b64 = base64.b64encode(bytes(additional_kp)).decode()

        signed_b64 = signer.sign_serialized_transaction(
            tx_b64,
            additional_signers=[additional_signer_b64],
        )

        # Verify signed tx has 2 non-default signatures
        signed_bytes = base64.b64decode(signed_b64)
        signed_tx = VersionedTransaction.from_bytes(signed_bytes)
        assert len(signed_tx.signatures) == 2
        assert signed_tx.signatures[0] != Signature.default()
        assert signed_tx.signatures[1] != Signature.default()

    def test_sign_without_additional_signers_unchanged(self, signer, unsigned_tx_base64):
        """Passing None or empty list for additional_signers works like before."""
        signed_none = signer.sign_serialized_transaction(unsigned_tx_base64, additional_signers=None)
        signed_empty = signer.sign_serialized_transaction(unsigned_tx_base64, additional_signers=[])

        # Both should produce valid signed transactions
        for signed_b64 in [signed_none, signed_empty]:
            signed_bytes = base64.b64decode(signed_b64)
            signed_tx = VersionedTransaction.from_bytes(signed_bytes)
            assert signed_tx.signatures[0] != Signature.default()

    def test_invalid_additional_signer_raises(self, signer, unsigned_tx_base64):
        """Invalid additional signer bytes should raise SolanaSignerError."""
        bad_signer = base64.b64encode(b"not-a-valid-keypair").decode()
        with pytest.raises(SolanaSignerError, match="Failed to sign"):
            signer.sign_serialized_transaction(
                unsigned_tx_base64,
                additional_signers=[bad_signer],
            )


# ---------------------------------------------------------------------------
# Message signing tests
# ---------------------------------------------------------------------------


class TestSolanaSignerMessage:
    def test_sign_message(self, signer):
        message = b"Hello Solana"
        sig = signer.sign_message(message)
        assert len(sig) == 64  # Ed25519 signatures are 64 bytes

    def test_different_messages_produce_different_sigs(self, signer):
        sig1 = signer.sign_message(b"message one")
        sig2 = signer.sign_message(b"message two")
        assert sig1 != sig2
