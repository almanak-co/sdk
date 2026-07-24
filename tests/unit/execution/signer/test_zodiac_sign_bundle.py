"""Unit tests for ``ZodiacSigner.sign_bundle_with_web3``.

Covers every branch of the bundle assembly path with the MultiSend encoder and
the Zodiac wrapper-signing step mocked at their seams — no RPC, no signing:

- empty bundle rejection
- nonce cache cleared at bundle start
- MultiSend payload forwarded into the wrapper (to/data/value/operation)
- gas summed across all bundled transactions
- EIP-1559 vs legacy gas-parameter propagation into the MultiSend tx
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from eth_account import Account
from eth_utils import to_checksum_address

from almanak.framework.execution.interfaces import (
    SignedTransaction,
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.signer.safe import zodiac as zodiac_mod
from almanak.framework.execution.signer.safe.config import (
    SafeSignerConfig,
    SafeWalletConfig,
)
from almanak.framework.execution.signer.safe.constants import SafeOperation
from almanak.framework.execution.signer.safe.multisend import MultiSendPayload
from almanak.framework.execution.signer.safe.zodiac import ZodiacSigner

# Well-known Anvil account #0 (public test key, never used on mainnet)
ANVIL_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
EOA = Account.from_key(ANVIL_KEY).address
SAFE = to_checksum_address("0x" + "98" * 20)
ZODIAC = to_checksum_address("0x" + "77" * 20)
TARGET = to_checksum_address("0x" + "44" * 20)
MULTISEND = to_checksum_address("0x38869bf66a61cf6bdb996a6ae40d5853fd43b526")
CHAIN_ID = 42161


def make_signer() -> ZodiacSigner:
    config = SafeSignerConfig(
        mode="zodiac",
        wallet_config=SafeWalletConfig(
            safe_address=SAFE,
            eoa_address=EOA,
            zodiac_roles_address=ZODIAC,
        ),
        private_key=ANVIL_KEY,
    )
    return ZodiacSigner(config)


def _eip1559_tx(gas_limit: int = 100_000) -> UnsignedTransaction:
    return UnsignedTransaction(
        to=TARGET,
        value=0,
        data="0xdeadbeef",
        chain_id=CHAIN_ID,
        gas_limit=gas_limit,
        tx_type=TransactionType.EIP_1559,
        max_fee_per_gas=100_000_000,
        max_priority_fee_per_gas=1_000_000,
    )


def _legacy_tx(gas_limit: int = 90_000) -> UnsignedTransaction:
    return UnsignedTransaction(
        to=TARGET,
        value=0,
        data="0xdeadbeef",
        chain_id=CHAIN_ID,
        gas_limit=gas_limit,
        tx_type=TransactionType.LEGACY,
        gas_price=2_000_000_000,
    )


def _payload() -> MultiSendPayload:
    return MultiSendPayload(
        to=MULTISEND,
        data="0x8d80ff0a" + "00" * 32,
        value=0,
        operation=SafeOperation.DELEGATE_CALL,
    )


def _sign_bundle(signer: ZodiacSigner, txs, payload=None, sentinel=None):
    """Run sign_bundle_with_web3 with encoder + wrapper-signing seams mocked."""
    payload = payload or _payload()
    sentinel = sentinel or MagicMock(spec=SignedTransaction)
    web3 = MagicMock()
    wrapper_mock = AsyncMock(return_value=sentinel)

    with (
        patch.object(zodiac_mod.MultiSendEncoder, "build_payload", return_value=payload) as build_mock,
        patch.object(signer, "_sign_multisend_with_zodiac", wrapper_mock),
    ):
        result = asyncio.run(signer.sign_bundle_with_web3(txs, web3, eoa_nonce=7, chain="arbitrum"))

    return result, sentinel, build_mock, wrapper_mock, web3


class TestSignBundleWithWeb3:
    def test_empty_bundle_rejected(self) -> None:
        signer = make_signer()

        with pytest.raises(ValueError, match="empty transaction bundle"):
            asyncio.run(signer.sign_bundle_with_web3([], MagicMock(), eoa_nonce=0, chain="arbitrum"))

    def test_eip1559_bundle_builds_multisend_wrapper(self) -> None:
        signer = make_signer()
        signer._safe_nonce_cache["stale"] = 99  # must be cleared at bundle start
        txs = [_eip1559_tx(gas_limit=100_000), _eip1559_tx(gas_limit=50_000)]
        payload = _payload()

        result, sentinel, build_mock, wrapper_mock, web3 = _sign_bundle(signer, txs, payload=payload)

        assert result is sentinel
        assert signer._safe_nonce_cache == {}
        build_mock.assert_called_once_with(txs, "arbitrum", web3)

        multisend_tx, wrapper_web3, eoa_nonce, operation = wrapper_mock.await_args.args
        assert wrapper_web3 is web3
        assert eoa_nonce == 7
        assert operation == SafeOperation.DELEGATE_CALL

        assert multisend_tx.to == payload.to
        assert multisend_tx.data == payload.data
        assert multisend_tx.value == 0
        assert multisend_tx.chain_id == CHAIN_ID
        assert multisend_tx.gas_limit == 150_000  # summed across the bundle
        assert multisend_tx.tx_type == TransactionType.EIP_1559
        assert multisend_tx.from_address == SAFE
        assert multisend_tx.max_fee_per_gas == 100_000_000
        assert multisend_tx.max_priority_fee_per_gas == 1_000_000
        assert multisend_tx.gas_price is None

    def test_legacy_bundle_propagates_gas_price(self) -> None:
        signer = make_signer()
        txs = [_legacy_tx(gas_limit=90_000), _legacy_tx(gas_limit=10_000), _legacy_tx(gas_limit=5_000)]

        result, sentinel, _, wrapper_mock, _ = _sign_bundle(signer, txs)

        assert result is sentinel
        (multisend_tx, *_), _ = wrapper_mock.await_args
        assert multisend_tx.gas_limit == 105_000
        assert multisend_tx.tx_type == TransactionType.LEGACY
        assert multisend_tx.gas_price == 2_000_000_000
        assert multisend_tx.max_fee_per_gas is None
        assert multisend_tx.max_priority_fee_per_gas is None
