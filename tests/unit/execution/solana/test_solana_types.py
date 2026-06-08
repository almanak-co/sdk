"""Unit tests for the shared Solana transaction payload types."""

from __future__ import annotations

from almanak.framework.execution.solana.types import SolanaTransactionData

_TO_DICT_KEYS = {
    "serialized_transaction",
    "chain_family",
    "tx_type",
    "description",
    "last_valid_block_height",
    "priority_fee_lamports",
}


class TestSolanaTransactionData:
    def test_defaults(self) -> None:
        tx = SolanaTransactionData(serialized_transaction="b64")
        assert tx.serialized_transaction == "b64"
        assert tx.chain_family == "SOLANA"
        assert tx.tx_type == "swap"
        assert tx.description == ""
        assert tx.last_valid_block_height == 0
        assert tx.priority_fee_lamports == 0

    def test_to_dict_shape_and_values(self) -> None:
        tx = SolanaTransactionData(
            serialized_transaction="b64data",
            tx_type="lp_open",
            description="Open LP",
            last_valid_block_height=280_000_000,
            priority_fee_lamports=5000,
        )
        assert tx.to_dict() == {
            "serialized_transaction": "b64data",
            "chain_family": "SOLANA",
            "tx_type": "lp_open",
            "description": "Open LP",
            "last_valid_block_height": 280_000_000,
            "priority_fee_lamports": 5000,
        }

    def test_to_dict_key_set_is_stable(self) -> None:
        assert set(SolanaTransactionData(serialized_transaction="b64").to_dict()) == _TO_DICT_KEYS

    def test_to_dict_is_repeatable(self) -> None:
        tx = SolanaTransactionData(serialized_transaction="b64")
        assert tx.to_dict() == tx.to_dict()
