"""Unit tests for DirectSafeSigner.sign_with_web3 and the MultiSend bundle path.

Covers almanak/framework/execution/signer/safe/direct.py sign_with_web3:

- happy path (EIP-1559) with a real eth-account key: ownership verification,
  Safe nonce read, getTransactionHash call shape, gas estimation, and a
  recoverable signed wrapper transaction
- ownership verification cached after the first call; bundle position uses
  cached Safe nonce
- legacy (gasPrice) gas-parameter branch
- contract-creation (to=None) rejection
- Enso delegate target uses DELEGATECALL
- sign_transaction failure wrapped in SigningError
- "0x" prefix normalization branches for raw_tx / tx_hash

And sign_bundle_with_web3 / _sign_multisend_with_web3:

- empty bundle rejection; nonce-cache clear on bundle start
- MultiSend payload shape (DELEGATECALL, multiSend selector, summed gas)
- EIP-1559 vs legacy gas-parameter propagation into the multisend tx
- missing 'to' rejection, sign failure wrapping, hex-prefix normalization

All web3 interaction is mocked; no RPC.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from eth_account import Account
from eth_utils import to_checksum_address
from web3 import Web3

from almanak.framework.execution.interfaces import (
    SignedTransaction,
    SigningError,
    TransactionType,
    UnsignedTransaction,
)
from almanak.framework.execution.signer.safe.config import (
    SafeSignerConfig,
    SafeWalletConfig,
)
from almanak.framework.execution.signer.safe.constants import (
    MULTISEND_SELECTOR,
    SAFE_EXEC_TRANSACTION_ABI,
    SAFE_GET_OWNERS_ABI,
    SAFE_GET_THRESHOLD_ABI,
    SAFE_GET_TX_HASH_ABI,
    SAFE_NONCE_ABI,
    ZERO_ADDRESS,
    SafeOperation,
    get_multisend_address,
)
from almanak.framework.execution.signer.safe.direct import DirectSafeSigner

# Well-known Anvil account #0 (public test key, never used on mainnet)
ANVIL_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
EOA = Account.from_key(ANVIL_KEY).address
SAFE = to_checksum_address("0x" + "98" * 20)
TARGET = to_checksum_address("0x" + "44" * 20)
# From ENSO_DELEGATE_ADDRESSES (requires DELEGATECALL)
ENSO_DELEGATE = to_checksum_address("0x7663fd40081dccd47805c00e613b6beac3b87f08")
CHAIN_ID = 31337
SAFE_TX_HASH = b"\x11" * 32


class _AwaitableValue:
    """A re-awaitable value, for mocking `await web3.eth.chain_id`."""

    def __init__(self, value):
        self._value = value

    def __await__(self):
        async def _get():
            return self._value

        return _get().__await__()


def make_signer() -> DirectSafeSigner:
    config = SafeSignerConfig(
        mode="direct",
        wallet_config=SafeWalletConfig(safe_address=SAFE, eoa_address=EOA),
        private_key=ANVIL_KEY,
    )
    return DirectSafeSigner(config)


def make_web3(
    safe_nonce: int = 5,
    owners: list[str] | None = None,
    threshold: int = 1,
    estimate: int = 400_000,
):
    """Build a MagicMock web3 whose eth.contract dispatches per Safe ABI."""
    web3 = MagicMock()
    web3.to_checksum_address = MagicMock(side_effect=Web3.to_checksum_address)
    web3.to_hex = MagicMock(side_effect=Web3.to_hex)

    eth = MagicMock()
    web3.eth = eth
    eth.chain_id = _AwaitableValue(CHAIN_ID)
    eth.estimate_gas = AsyncMock(return_value=estimate)

    owners_contract = MagicMock()
    owners_contract.functions.getOwners.return_value.call = AsyncMock(return_value=list(owners if owners is not None else [EOA]))

    threshold_contract = MagicMock()
    threshold_contract.functions.getThreshold.return_value.call = AsyncMock(return_value=threshold)

    nonce_contract = MagicMock()
    nonce_contract.functions.nonce.return_value.call = AsyncMock(return_value=safe_nonce)

    hash_contract = MagicMock()
    hash_contract.functions.getTransactionHash.return_value.call = AsyncMock(return_value=SAFE_TX_HASH)

    exec_contract = MagicMock()
    built_wrappers: list[dict] = []

    async def _build(params):
        wrapper = {"to": SAFE, "data": "0x" + "ab" * 40, "chainId": CHAIN_ID}
        wrapper.update(params)
        built_wrappers.append(wrapper)
        return wrapper

    exec_contract.functions.execTransaction.return_value.build_transaction = AsyncMock(side_effect=_build)

    def _contract(address=None, abi=None):
        if abi is SAFE_GET_OWNERS_ABI:
            return owners_contract
        if abi is SAFE_GET_THRESHOLD_ABI:
            return threshold_contract
        if abi is SAFE_NONCE_ABI:
            return nonce_contract
        if abi is SAFE_GET_TX_HASH_ABI:
            return hash_contract
        if abi is SAFE_EXEC_TRANSACTION_ABI:
            return exec_contract
        raise AssertionError(f"Unexpected ABI requested: {abi}")

    eth.contract = MagicMock(side_effect=_contract)

    # Expose sub-mocks for assertions
    web3.mock_owners = owners_contract
    web3.mock_threshold = threshold_contract
    web3.mock_nonce = nonce_contract
    web3.mock_hash = hash_contract
    web3.mock_exec = exec_contract
    web3.built_wrappers = built_wrappers
    return web3


def make_tx(
    to: str | None = TARGET,
    value: int = 0,
    data: str = "0xdeadbeef",
    tx_type: TransactionType = TransactionType.EIP_1559,
) -> UnsignedTransaction:
    return UnsignedTransaction(
        to=to,
        value=value,
        data=data,
        chain_id=CHAIN_ID,
        gas_limit=200_000,
        tx_type=tx_type,
        max_fee_per_gas=2_000_000_000 if tx_type == TransactionType.EIP_1559 else None,
        max_priority_fee_per_gas=1_000_000 if tx_type == TransactionType.EIP_1559 else None,
        gas_price=3_000_000_000 if tx_type == TransactionType.LEGACY else None,
    )


def make_mock_account(raw_hex: str, hash_hex: str) -> MagicMock:
    """Account stub with a real key (for Safe signature) and canned sign output."""
    account = MagicMock()
    account.key = bytes.fromhex(ANVIL_KEY[2:])
    signed = MagicMock()
    signed.raw_transaction.hex.return_value = raw_hex
    signed.hash.hex.return_value = hash_hex
    account.sign_transaction.return_value = signed
    return account


# =============================================================================
# Constructor
# =============================================================================


def test_init_rejects_non_direct_mode():
    config = SafeSignerConfig(
        mode="zodiac",
        wallet_config=SafeWalletConfig(
            safe_address=SAFE,
            eoa_address=EOA,
            zodiac_roles_address=to_checksum_address("0x" + "55" * 20),
        ),
        private_key=ANVIL_KEY,
    )
    with pytest.raises(ValueError, match="requires mode='direct'"):
        DirectSafeSigner(config)


# =============================================================================
# sign_with_web3
# =============================================================================


@pytest.mark.asyncio
async def test_happy_path_eip1559():
    signer = make_signer()
    web3 = make_web3(safe_nonce=5, estimate=400_000)
    tx = make_tx()

    result = await signer.sign_with_web3(tx, web3, eoa_nonce=9)

    assert isinstance(result, SignedTransaction)
    assert result.unsigned_tx is tx
    assert result.raw_tx.startswith("0x")
    assert result.tx_hash.startswith("0x")
    # The wrapper is genuinely signed by the EOA key
    assert Account.recover_transaction(result.raw_tx) == EOA
    assert signer._ownership_verified is True

    # getTransactionHash called with CALL operation and chain Safe nonce
    web3.mock_hash.functions.getTransactionHash.assert_called_once_with(
        TARGET,
        0,
        "0xdeadbeef",
        SafeOperation.CALL,
        0,
        0,
        0,
        ZERO_ADDRESS,
        ZERO_ADDRESS,
        5,
    )

    # execTransaction signature is 65 bytes (r + s + v), v in {27, 28}
    (exec_args, _) = web3.mock_exec.functions.execTransaction.call_args
    signature = exec_args[-1]
    assert len(signature) == 65
    assert signature[64] in (27, 28)

    # Wrapper built with EIP-1559 gas params and gas replaced by estimate * 1.3
    wrapper = web3.built_wrappers[0]
    assert wrapper["type"] == 2
    assert wrapper["maxFeePerGas"] == tx.max_fee_per_gas
    assert wrapper["maxPriorityFeePerGas"] == tx.max_priority_fee_per_gas
    assert wrapper["nonce"] == 9
    assert wrapper["value"] == 0
    assert wrapper["gas"] == int(400_000 * 1.3)


@pytest.mark.asyncio
async def test_second_call_skips_ownership_and_uses_cached_bundle_nonce():
    signer = make_signer()
    web3 = make_web3(safe_nonce=5)

    await signer.sign_with_web3(make_tx(), web3, eoa_nonce=1, pos_in_bundle=0)
    await signer.sign_with_web3(make_tx(), web3, eoa_nonce=2, pos_in_bundle=1)

    # Ownership verified exactly once (cached for the session)
    web3.mock_owners.functions.getOwners.assert_called_once()
    web3.mock_threshold.functions.getThreshold.assert_called_once()
    # Safe nonce read from chain only for pos=0; pos=1 uses cache + offset
    web3.mock_nonce.functions.nonce.assert_called_once()
    nonces_used = [call.args[-1] for call in web3.mock_hash.functions.getTransactionHash.call_args_list]
    assert nonces_used == [5, 6]


@pytest.mark.asyncio
async def test_legacy_gas_params_branch():
    signer = make_signer()
    signer._ownership_verified = True
    web3 = make_web3(estimate=100_000)
    tx = make_tx(tx_type=TransactionType.LEGACY)

    result = await signer.sign_with_web3(tx, web3, eoa_nonce=3)

    wrapper = web3.built_wrappers[0]
    assert wrapper["gasPrice"] == tx.gas_price
    assert "type" not in wrapper
    assert "maxFeePerGas" not in wrapper
    assert Account.recover_transaction(result.raw_tx) == EOA


@pytest.mark.asyncio
async def test_contract_creation_rejected():
    signer = make_signer()
    signer._ownership_verified = True
    web3 = make_web3()

    with pytest.raises(SigningError, match="Contract creation not supported"):
        await signer.sign_with_web3(make_tx(to=None), web3, eoa_nonce=0)


@pytest.mark.asyncio
async def test_enso_delegate_target_uses_delegatecall():
    signer = make_signer()
    signer._ownership_verified = True
    web3 = make_web3()

    await signer.sign_with_web3(make_tx(to=ENSO_DELEGATE), web3, eoa_nonce=0)

    (hash_args, _) = web3.mock_hash.functions.getTransactionHash.call_args
    assert hash_args[3] == SafeOperation.DELEGATE_CALL


@pytest.mark.asyncio
async def test_sign_transaction_failure_wrapped_in_signing_error():
    signer = make_signer()
    signer._ownership_verified = True
    web3 = make_web3()

    account = make_mock_account("dead", "beef")
    account.sign_transaction.side_effect = RuntimeError("kaput")
    signer._account = account

    with pytest.raises(SigningError, match="Failed to sign Safe transaction: RuntimeError: kaput"):
        await signer.sign_with_web3(make_tx(), web3, eoa_nonce=0)


@pytest.mark.asyncio
async def test_adds_hex_prefix_when_missing():
    signer = make_signer()
    signer._ownership_verified = True
    web3 = make_web3()
    signer._account = make_mock_account("f8aa11", "cafe")

    result = await signer.sign_with_web3(make_tx(), web3, eoa_nonce=0)

    assert result.raw_tx == "0xf8aa11"
    assert result.tx_hash == "0xcafe"


@pytest.mark.asyncio
async def test_keeps_existing_hex_prefix():
    signer = make_signer()
    signer._ownership_verified = True
    web3 = make_web3()
    signer._account = make_mock_account("0xf8aa11", "0xcafe")

    result = await signer.sign_with_web3(make_tx(), web3, eoa_nonce=0)

    assert result.raw_tx == "0xf8aa11"
    assert result.tx_hash == "0xcafe"


@pytest.mark.asyncio
async def test_eoa_not_owner_raises():
    signer = make_signer()
    web3 = make_web3(owners=[to_checksum_address("0x" + "66" * 20)])

    with pytest.raises(SigningError, match="is not an owner of Safe"):
        await signer.sign_with_web3(make_tx(), web3, eoa_nonce=0)
    assert signer._ownership_verified is False


@pytest.mark.asyncio
async def test_threshold_above_one_raises():
    signer = make_signer()
    web3 = make_web3(threshold=2)

    with pytest.raises(SigningError, match="threshold=2"):
        await signer.sign_with_web3(make_tx(), web3, eoa_nonce=0)


# =============================================================================
# sign_bundle_with_web3
# =============================================================================

MULTISEND_ETHEREUM = to_checksum_address(get_multisend_address("ethereum"))


@pytest.mark.asyncio
async def test_bundle_empty_list_raises_value_error():
    signer = make_signer()

    with pytest.raises(ValueError, match="empty transaction bundle"):
        await signer.sign_bundle_with_web3([], make_web3(), eoa_nonce=0, chain="ethereum")


@pytest.mark.asyncio
async def test_bundle_happy_path_eip1559():
    signer = make_signer()
    web3 = make_web3(safe_nonce=7, estimate=500_000)
    tx1 = make_tx()
    tx2 = make_tx(value=5, data="0xbeef01")

    result = await signer.sign_bundle_with_web3([tx1, tx2], web3, eoa_nonce=4, chain="ethereum")

    assert isinstance(result, SignedTransaction)
    assert Account.recover_transaction(result.raw_tx) == EOA

    # The unsigned multisend tx targets the chain's MultiSend contract with
    # summed inner gas and first-tx EIP-1559 gas params
    ms_tx = result.unsigned_tx
    assert ms_tx.to == MULTISEND_ETHEREUM
    assert ms_tx.value == 0
    assert ms_tx.data.startswith(MULTISEND_SELECTOR)
    assert ms_tx.gas_limit == tx1.gas_limit + tx2.gas_limit
    assert ms_tx.from_address == SAFE
    assert ms_tx.tx_type == TransactionType.EIP_1559
    assert ms_tx.max_fee_per_gas == tx1.max_fee_per_gas
    assert ms_tx.max_priority_fee_per_gas == tx1.max_priority_fee_per_gas
    assert ms_tx.gas_price is None

    # getTransactionHash uses DELEGATECALL and the chain Safe nonce (pos=0)
    (hash_args, _) = web3.mock_hash.functions.getTransactionHash.call_args
    assert hash_args[0] == MULTISEND_ETHEREUM
    assert hash_args[3] == SafeOperation.DELEGATE_CALL
    assert hash_args[-1] == 7

    # execTransaction mirrors the DELEGATECALL operation with a 65-byte sig
    (exec_args, _) = web3.mock_exec.functions.execTransaction.call_args
    assert exec_args[3] == SafeOperation.DELEGATE_CALL
    signature = exec_args[-1]
    assert len(signature) == 65
    assert signature[64] in (27, 28)

    # Wrapper: EOA nonce, EIP-1559 marker, buffered estimate
    wrapper = web3.built_wrappers[0]
    assert wrapper["nonce"] == 4
    assert wrapper["type"] == 2
    assert wrapper["value"] == 0
    assert wrapper["gas"] == int(500_000 * 1.3)


@pytest.mark.asyncio
async def test_bundle_legacy_gas_params():
    signer = make_signer()
    web3 = make_web3()
    tx = make_tx(tx_type=TransactionType.LEGACY)

    result = await signer.sign_bundle_with_web3([tx], web3, eoa_nonce=1, chain="ethereum")

    ms_tx = result.unsigned_tx
    assert ms_tx.tx_type == TransactionType.LEGACY
    assert ms_tx.gas_price == tx.gas_price
    assert ms_tx.max_fee_per_gas is None
    assert ms_tx.max_priority_fee_per_gas is None

    wrapper = web3.built_wrappers[0]
    assert wrapper["gasPrice"] == tx.gas_price
    assert "type" not in wrapper
    assert "maxFeePerGas" not in wrapper
    assert Account.recover_transaction(result.raw_tx) == EOA


@pytest.mark.asyncio
async def test_bundle_clears_stale_nonce_cache():
    signer = make_signer()
    web3 = make_web3(safe_nonce=7)
    signer._safe_nonce_cache["stale-key"] = 99

    await signer.sign_bundle_with_web3([make_tx()], web3, eoa_nonce=0, chain="ethereum")

    # Stale entries are dropped at bundle start; the fresh chain nonce is used
    assert "stale-key" not in signer._safe_nonce_cache
    (hash_args, _) = web3.mock_hash.functions.getTransactionHash.call_args
    assert hash_args[-1] == 7


# =============================================================================
# _sign_multisend_with_web3
# =============================================================================


@pytest.mark.asyncio
async def test_multisend_missing_to_raises():
    signer = make_signer()

    with pytest.raises(SigningError, match="requires 'to' address"):
        await signer._sign_multisend_with_web3(
            make_tx(to=None), make_web3(), eoa_nonce=0, operation=SafeOperation.DELEGATE_CALL
        )


@pytest.mark.asyncio
async def test_multisend_sign_failure_wrapped_in_signing_error():
    signer = make_signer()
    web3 = make_web3()
    account = make_mock_account("dead", "beef")
    account.sign_transaction.side_effect = RuntimeError("kaput")
    signer._account = account

    with pytest.raises(SigningError, match="Failed to sign MultiSend bundle: RuntimeError: kaput"):
        await signer._sign_multisend_with_web3(
            make_tx(), web3, eoa_nonce=0, operation=SafeOperation.DELEGATE_CALL
        )


@pytest.mark.asyncio
async def test_multisend_adds_hex_prefix_when_missing():
    signer = make_signer()
    web3 = make_web3()
    signer._account = make_mock_account("f8bb22", "feed")

    result = await signer._sign_multisend_with_web3(
        make_tx(), web3, eoa_nonce=0, operation=SafeOperation.DELEGATE_CALL
    )

    assert result.raw_tx == "0xf8bb22"
    assert result.tx_hash == "0xfeed"


@pytest.mark.asyncio
async def test_multisend_keeps_existing_hex_prefix():
    signer = make_signer()
    web3 = make_web3()
    signer._account = make_mock_account("0xf8bb22", "0xfeed")

    result = await signer._sign_multisend_with_web3(
        make_tx(), web3, eoa_nonce=0, operation=SafeOperation.DELEGATE_CALL
    )

    assert result.raw_tx == "0xf8bb22"
    assert result.tx_hash == "0xfeed"


@pytest.mark.asyncio
async def test_multisend_legacy_gas_params_branch():
    signer = make_signer()
    web3 = make_web3(estimate=100_000)
    tx = make_tx(tx_type=TransactionType.LEGACY)

    result = await signer._sign_multisend_with_web3(
        tx, web3, eoa_nonce=3, operation=SafeOperation.DELEGATE_CALL
    )

    wrapper = web3.built_wrappers[0]
    assert wrapper["gasPrice"] == tx.gas_price
    assert "type" not in wrapper
    assert wrapper["gas"] == int(100_000 * 1.3)
    assert Account.recover_transaction(result.raw_tx) == EOA
