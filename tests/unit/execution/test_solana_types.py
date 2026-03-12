"""Tests for Solana transaction data types.

Verifies construction, immutability, and field defaults for all Solana types.
These types are pure Python dataclasses — no external dependencies required.
"""

import pytest

from almanak.framework.execution.solana.types import (
    AccountMeta,
    SignedSolanaTransaction,
    SolanaInstruction,
    SolanaTransaction,
    SolanaTransactionReceipt,
)


class TestAccountMeta:
    def test_construction(self):
        meta = AccountMeta(pubkey="11111111111111111111111111111111", is_signer=True, is_writable=False)
        assert meta.pubkey == "11111111111111111111111111111111"
        assert meta.is_signer is True
        assert meta.is_writable is False

    def test_frozen(self):
        meta = AccountMeta(pubkey="abc", is_signer=False, is_writable=True)
        with pytest.raises(AttributeError):
            meta.pubkey = "xyz"

    def test_equality(self):
        a = AccountMeta(pubkey="abc", is_signer=True, is_writable=False)
        b = AccountMeta(pubkey="abc", is_signer=True, is_writable=False)
        assert a == b


class TestSolanaInstruction:
    def test_construction_defaults(self):
        ix = SolanaInstruction(program_id="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
        assert ix.program_id == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
        assert ix.accounts == ()
        assert ix.data == b""

    def test_construction_with_accounts(self):
        meta = AccountMeta(pubkey="abc", is_signer=True, is_writable=True)
        ix = SolanaInstruction(
            program_id="prog",
            accounts=(meta,),
            data=b"\x01\x02",
        )
        assert len(ix.accounts) == 1
        assert ix.accounts[0].pubkey == "abc"
        assert ix.data == b"\x01\x02"

    def test_frozen(self):
        ix = SolanaInstruction(program_id="prog")
        with pytest.raises(AttributeError):
            ix.program_id = "other"


class TestSolanaTransaction:
    def test_defaults(self):
        tx = SolanaTransaction()
        assert tx.instructions == []
        assert tx.fee_payer == ""
        assert tx.recent_blockhash == ""
        assert tx.address_lookup_tables == []
        assert tx.compute_units == 0
        assert tx.priority_fee_lamports == 0
        assert tx.metadata == {}

    def test_construction_full(self):
        ix = SolanaInstruction(program_id="prog")
        tx = SolanaTransaction(
            instructions=[ix],
            fee_payer="FeePayerPubkey",
            recent_blockhash="GHtXQBsoZHVnNFa9YevAzFr17DJjgHXk3ycTKD5xD3Zi",
            address_lookup_tables=["LUT1"],
            compute_units=200_000,
            priority_fee_lamports=5000,
            metadata={"intent_id": "test"},
        )
        assert len(tx.instructions) == 1
        assert tx.fee_payer == "FeePayerPubkey"
        assert tx.compute_units == 200_000
        assert tx.metadata["intent_id"] == "test"

    def test_mutable(self):
        tx = SolanaTransaction()
        ix = SolanaInstruction(program_id="prog")
        tx.instructions.append(ix)
        assert len(tx.instructions) == 1
        tx.fee_payer = "new_payer"
        assert tx.fee_payer == "new_payer"


class TestSignedSolanaTransaction:
    def test_construction(self):
        unsigned = SolanaTransaction(fee_payer="payer")
        signed = SignedSolanaTransaction(
            raw_tx=b"\x00\x01\x02",
            signature="5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQN",
            unsigned_tx=unsigned,
        )
        assert signed.raw_tx == b"\x00\x01\x02"
        assert signed.signature.startswith("5VERv8")
        assert signed.unsigned_tx.fee_payer == "payer"

    def test_frozen(self):
        unsigned = SolanaTransaction()
        signed = SignedSolanaTransaction(raw_tx=b"", signature="sig", unsigned_tx=unsigned)
        with pytest.raises(AttributeError):
            signed.signature = "other"


class TestSolanaTransactionReceipt:
    def test_defaults(self):
        receipt = SolanaTransactionReceipt(signature="sig123", slot=42)
        assert receipt.signature == "sig123"
        assert receipt.slot == 42
        assert receipt.block_time is None
        assert receipt.fee_lamports == 0
        assert receipt.success is True
        assert receipt.err is None
        assert receipt.logs == []
        assert receipt.pre_token_balances == []
        assert receipt.post_token_balances == []

    def test_failed_receipt(self):
        receipt = SolanaTransactionReceipt(
            signature="fail_sig",
            slot=100,
            fee_lamports=5000,
            success=False,
            err={"InstructionError": [0, "Custom"]},
            logs=["Program log: Error"],
        )
        assert receipt.success is False
        assert receipt.err is not None
        assert "InstructionError" in receipt.err
        assert len(receipt.logs) == 1

    def test_with_token_balances(self):
        pre = [{"mint": "USDC", "amount": "1000000"}]
        post = [{"mint": "USDC", "amount": "500000"}]
        receipt = SolanaTransactionReceipt(
            signature="sig",
            slot=200,
            pre_token_balances=pre,
            post_token_balances=post,
        )
        assert receipt.pre_token_balances[0]["amount"] == "1000000"
        assert receipt.post_token_balances[0]["amount"] == "500000"
