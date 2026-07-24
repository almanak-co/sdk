"""Branch coverage for ChainExecutor.sign_transaction.

Signs real transactions offline with the well-known Anvil dev key (both
EIP-1559 and legacy shapes), and drives the defensive branches (missing
nonce, unextractable raw tx, string attributes, signer failure) with a
stubbed account. No RPC access.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.chain_executor import ChainExecutor
from almanak.framework.execution.interfaces import (
    SigningError,
    TransactionType,
    UnsignedTransaction,
)

# Anvil's first well-known dev account key (public knowledge, test-only).
_TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"


@pytest.fixture
def executor() -> ChainExecutor:
    return ChainExecutor(
        chain="arbitrum",
        rpc_url="https://example.com",
        private_key=_TEST_PRIVATE_KEY,
    )


def _unsigned(*, tx_type=TransactionType.EIP_1559, nonce=1, **overrides):
    from eth_utils import to_checksum_address

    fields = {
        # eth-account requires EIP-55 checksummed addresses.
        "to": to_checksum_address("0x" + "aa" * 20),
        "value": 10,
        "data": "0x",
        "chain_id": 42161,
        "gas_limit": 21000,
        "nonce": nonce,
        "tx_type": tx_type,
    }
    if tx_type == TransactionType.EIP_1559:
        fields.update(max_fee_per_gas=100, max_priority_fee_per_gas=2)
    else:
        fields.update(gas_price=50)
    fields.update(overrides)
    return UnsignedTransaction(**fields)


class TestSignTransaction:
    def test_missing_nonce_rejected(self, executor):
        with pytest.raises(SigningError, match="nonce must be set"):
            asyncio.run(executor.sign_transaction(_unsigned(nonce=None)))

    def test_signs_eip1559_transaction(self, executor):
        tx = _unsigned()
        signed = asyncio.run(executor.sign_transaction(tx))
        assert signed.raw_tx.startswith("0x")
        assert signed.tx_hash.startswith("0x")
        assert len(signed.tx_hash) == 66  # 32-byte hash
        assert signed.unsigned_tx is tx
        # EIP-1559 payloads are type-2 envelopes.
        assert signed.raw_tx.startswith("0x02")

    def test_signs_legacy_transaction(self, executor):
        signed = asyncio.run(executor.sign_transaction(_unsigned(tx_type=TransactionType.LEGACY)))
        assert signed.raw_tx.startswith("0x")
        assert not signed.raw_tx.startswith("0x02")

    def test_unextractable_raw_transaction_rejected(self, executor):
        executor._account = MagicMock()
        executor._account.sign_transaction.return_value = SimpleNamespace(hash=b"\xbe\xef")
        with pytest.raises(SigningError, match="Could not extract raw transaction"):
            asyncio.run(executor.sign_transaction(_unsigned()))

    def test_string_attributes_gain_hex_prefix(self, executor):
        executor._account = MagicMock()
        executor._account.sign_transaction.return_value = SimpleNamespace(
            raw_transaction="abcd", hash="beef"
        )
        signed = asyncio.run(executor.sign_transaction(_unsigned()))
        assert signed.raw_tx == "0xabcd"
        assert signed.tx_hash == "0xbeef"

    def test_signer_failure_wrapped_without_key_material(self, executor):
        executor._account = MagicMock()
        # The signer error deliberately embeds key material: the wrapped
        # SigningError must carry only the exception type, never the message.
        executor._account.sign_transaction.side_effect = ValueError(
            f"bad field in key {_TEST_PRIVATE_KEY}"
        )
        with pytest.raises(
            SigningError,
            match=r"^Signing failed: Failed to sign transaction on arbitrum: ValueError$",
        ) as excinfo:
            asyncio.run(executor.sign_transaction(_unsigned()))
        assert _TEST_PRIVATE_KEY not in str(excinfo.value)
        # `raise ... from None` severs the causal chain so the raw ValueError
        # cannot surface in rendered tracebacks either.
        assert excinfo.value.__cause__ is None
