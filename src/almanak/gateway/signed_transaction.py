"""Verify retained legacy envelopes without requesting another signature."""

from eth_account import Account
from eth_account._utils.legacy_transactions import Transaction
from eth_utils import keccak

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number


def verify_signed(pending, execution, transport_chain: ChainId):
    try:
        raw = bytes.fromhex(pending.raw_transaction[2:])
        tx = Transaction.from_bytes(raw)
        sender = Account.recover_transaction(raw).lower()
        actual = (
            tx.nonce,
            tx.gasPrice,
            tx.gas,
            tx.to.hex(),
            tx.value,
            tx.data.hex(),
            (tx.v - 35) // 2,
            sender,
            "0x" + keccak(raw).hex(),
        )
    except Exception:
        raise DataError(
            "inconsistent", "retained transaction is not a signed legacy envelope"
        ) from None
    expected = (
        pending.nonce,
        pending.gas_price,
        pending.gas,
        execution.to[2:],
        execution.value,
        execution.data[2:],
        chain_number(transport_chain),
        execution.submitter.address,
        pending.tx_hash,
    )
    if actual != expected or tx.v < 35:
        raise DataError("inconsistent", "persisted signed transaction identity is invalid")
