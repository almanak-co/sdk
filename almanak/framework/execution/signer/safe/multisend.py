"""MultiSend encoder for Safe atomic transaction bundles.

The MultiSend contract executes multiple transactions atomically.
It must be called via DELEGATECALL from the Safe so that:
1. msg.sender appears as the Safe address to target contracts
2. All transactions share the same execution context
3. All transactions succeed or all fail (atomic)

Encoding format per inner transaction (packed bytes):
- operation: 1 byte (0 = CALL, 1 = DELEGATECALL)
- to: 20 bytes (target address)
- value: 32 bytes (ETH value in wei, big-endian)
- dataLength: 32 bytes (length of data, big-endian)
- data: variable bytes (calldata)

Total per tx: 1 + 20 + 32 + 32 + len(data) = 85 + len(data) bytes

Reference:
https://github.com/safe-global/safe-smart-account/blob/main/contracts/libraries/MultiSend.sol

Example:
    from almanak.framework.execution.signer.safe.multisend import MultiSendEncoder

    # Encode multiple transactions
    payload = MultiSendEncoder.build_payload(
        transactions=[tx1, tx2, tx3],
        chain="arbitrum",
        web3=web3_instance,
    )

    # Use payload with Safe.execTransaction()
    safe.execTransaction(
        payload.to,
        payload.value,
        payload.data,
        payload.operation,
        ...
    )
"""

import logging
from dataclasses import dataclass
from typing import Any

from eth_abi import decode, encode
from web3 import AsyncWeb3, Web3

from almanak.framework.execution.interfaces import UnsignedTransaction
from almanak.framework.execution.signer.safe.constants import (
    MULTISEND_SELECTOR,
    SafeOperation,
    get_multisend_address,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class MultiSendPayload:
    """Payload for a MultiSend transaction.

    This dataclass represents the arguments needed for Safe.execTransaction()
    when executing a MultiSend bundle.

    Attributes:
        to: MultiSend contract address
        data: Encoded multiSend(bytes) calldata
        value: ETH value (always 0 for MultiSend wrapper)
        operation: Always DELEGATECALL for MultiSend

    Example:
        payload = MultiSendPayload(
            to="0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526",
            data="0x8d80ff0a...",
            value=0,
            operation=SafeOperation.DELEGATE_CALL,
        )
    """

    to: str
    data: str
    value: int = 0
    operation: SafeOperation = SafeOperation.DELEGATE_CALL


# =============================================================================
# MultiSend Encoder
# =============================================================================


class MultiSendEncoder:
    """Encodes multiple transactions into a MultiSend call.

    This encoder produces a MultiSendPayload that can be used with
    Safe.execTransaction() to execute multiple transactions atomically.

    The encoding process:
    1. Pack each transaction into the MultiSend format
    2. Concatenate all packed transactions
    3. ABI-encode the packed bytes
    4. Prepend the multiSend(bytes) function selector

    Example:
        # Encode transactions
        payload = MultiSendEncoder.build_payload(
            transactions=[approve_tx, swap_tx, add_liquidity_tx],
            chain="arbitrum",
            web3=web3_instance,
        )

        # Use with Safe.execTransaction
        # The Safe will DELEGATECALL to MultiSend
        # MultiSend will CALL each target contract
    """

    @staticmethod
    def encode_transactions(
        transactions: list[UnsignedTransaction],
        web3: Web3 | AsyncWeb3,
    ) -> bytes:
        """Encode a list of transactions into MultiSend calldata.

        This method packs transactions according to the MultiSend format:
        - For each transaction: operation(1) + to(20) + value(32) + dataLen(32) + data
        - Then wraps with multiSend(bytes) selector and ABI encoding

        Args:
            transactions: List of UnsignedTransaction objects to bundle
            web3: Web3 instance for address checksumming

        Returns:
            Complete calldata for MultiSend.multiSend(bytes)

        Raises:
            ValueError: If transactions list is empty
        """
        if not transactions:
            raise ValueError("Cannot encode empty transaction list")

        packed_data = b""

        for i, tx in enumerate(transactions):
            # All inner transactions use CALL (operation = 0)
            # The Safe will DELEGATECALL to MultiSend, which will CALL each target
            operation = 0

            # Target address (20 bytes)
            if tx.to is None:
                raise ValueError(f"Transaction {i} has no 'to' address")

            to_address = web3.to_checksum_address(tx.to)
            to_bytes = bytes.fromhex(to_address[2:])  # Remove 0x prefix

            # Value (32 bytes, big-endian)
            value = tx.value
            value_bytes = value.to_bytes(32, byteorder="big")

            # Data
            data = tx.data
            if isinstance(data, str):
                if data.startswith("0x"):
                    data_bytes = bytes.fromhex(data[2:])
                else:
                    data_bytes = bytes.fromhex(data) if data else b""
            else:
                data_bytes = data if data else b""

            # Data length (32 bytes, big-endian)
            data_length_bytes = len(data_bytes).to_bytes(32, byteorder="big")

            # Pack: operation (1) + to (20) + value (32) + dataLength (32) + data
            tx_packed = operation.to_bytes(1, byteorder="big") + to_bytes + value_bytes + data_length_bytes + data_bytes

            packed_data += tx_packed

            logger.debug(
                f"Encoded tx {i}: to={to_address}, value={value}, dataLen={len(data_bytes)}, totalLen={len(tx_packed)}"
            )

        # Encode as multiSend(bytes transactions)
        # The ABI encoding wraps the packed bytes with offset and length
        encoded_bytes = encode(["bytes"], [packed_data])

        # Prepend the function selector
        calldata = bytes.fromhex(MULTISEND_SELECTOR[2:]) + encoded_bytes

        logger.debug(
            f"Encoded {len(transactions)} transactions into MultiSend calldata, total length: {len(calldata)} bytes"
        )

        return calldata

    @staticmethod
    def encode_from_dicts(
        transactions: list[dict[str, Any]],
        web3: Web3 | AsyncWeb3,
    ) -> bytes:
        """Encode a list of transaction dicts into MultiSend calldata.

        Alternative to encode_transactions that accepts raw dictionaries
        instead of UnsignedTransaction objects.

        Args:
            transactions: List of transaction dicts with 'to', 'value', 'data' keys
            web3: Web3 instance for address checksumming

        Returns:
            Complete calldata for MultiSend.multiSend(bytes)

        Raises:
            ValueError: If transactions list is empty or missing required fields
        """
        if not transactions:
            raise ValueError("Cannot encode empty transaction list")

        packed_data = b""

        for i, tx_dict in enumerate(transactions):
            # All inner transactions use CALL (operation = 0)
            operation = 0

            # Target address (20 bytes)
            to_address = tx_dict.get("to")
            if not to_address:
                raise ValueError(f"Transaction {i} missing 'to' address")

            to_address = web3.to_checksum_address(to_address)
            to_bytes = bytes.fromhex(to_address[2:])

            # Value (32 bytes, big-endian)
            value = tx_dict.get("value", 0)
            if isinstance(value, str):
                if value.startswith("0x"):
                    value = int(value, 16)
                else:
                    value = int(value)
            value_bytes = value.to_bytes(32, byteorder="big")

            # Data
            data = tx_dict.get("data", "0x")
            if isinstance(data, str):
                if data.startswith("0x"):
                    data_bytes = bytes.fromhex(data[2:])
                else:
                    data_bytes = bytes.fromhex(data) if data else b""
            else:
                data_bytes = data if data else b""

            # Data length (32 bytes, big-endian)
            data_length_bytes = len(data_bytes).to_bytes(32, byteorder="big")

            # Pack: operation (1) + to (20) + value (32) + dataLength (32) + data
            tx_packed = operation.to_bytes(1, byteorder="big") + to_bytes + value_bytes + data_length_bytes + data_bytes

            packed_data += tx_packed

        # Encode as multiSend(bytes transactions)
        encoded_bytes = encode(["bytes"], [packed_data])

        # Prepend the function selector
        return bytes.fromhex(MULTISEND_SELECTOR[2:]) + encoded_bytes

    @staticmethod
    def build_payload(
        transactions: list[UnsignedTransaction],
        chain: str,
        web3: Web3 | AsyncWeb3,
    ) -> MultiSendPayload:
        """Build a MultiSendPayload for Safe.execTransaction().

        This returns a payload (not a transaction) that contains the
        arguments needed for Safe.execTransaction():
        - to: MultiSend contract address
        - data: Encoded multiSend(bytes) calldata
        - value: 0 (inner transactions have their own values)
        - operation: DELEGATECALL (always 1)

        The signer is responsible for:
        - Fetching Safe nonce
        - Fetching EOA nonce
        - Building the actual EOA -> Safe transaction
        - Gas estimation on the final wrapped transaction
        - Signing

        Args:
            transactions: List of transactions to bundle
            chain: Target chain (for MultiSend address lookup)
            web3: Web3 instance

        Returns:
            MultiSendPayload for Safe.execTransaction()

        Raises:
            ValueError: If transactions list is empty
        """
        if not transactions:
            raise ValueError("Cannot build payload from empty transaction list")

        calldata = MultiSendEncoder.encode_transactions(transactions, web3)
        multisend_address = get_multisend_address(chain)

        payload = MultiSendPayload(
            to=web3.to_checksum_address(multisend_address),
            data="0x" + calldata.hex(),
            value=0,  # Value is encoded in inner transactions
            operation=SafeOperation.DELEGATE_CALL,  # Always DELEGATECALL for MultiSend
        )

        logger.debug(f"Built MultiSend payload: to={payload.to}, dataLen={len(payload.data)}, operation=DELEGATECALL")

        return payload

    @staticmethod
    def decode_multisend_data(calldata: bytes | str) -> list[dict[str, Any]]:
        """Decode MultiSend calldata back into individual transaction dicts.

        Useful for debugging and verification.

        Args:
            calldata: The encoded multiSend(bytes) calldata

        Returns:
            List of dicts with keys: operation, to, value, data

        Raises:
            ValueError: If calldata is invalid
        """
        # Convert string to bytes if needed
        if isinstance(calldata, str):
            if calldata.startswith("0x"):
                calldata = bytes.fromhex(calldata[2:])
            else:
                calldata = bytes.fromhex(calldata)

        # Skip the 4-byte selector
        if len(calldata) < 4:
            raise ValueError("Calldata too short")

        selector = calldata[:4].hex()
        expected_selector = MULTISEND_SELECTOR[2:]
        if selector != expected_selector:
            raise ValueError(f"Invalid selector: 0x{selector}")

        # Decode the bytes parameter
        # ABI encoding: offset (32 bytes) + length (32 bytes) + data
        encoded_data = calldata[4:]
        (packed_txs,) = decode(["bytes"], encoded_data)

        transactions = []
        i = 0

        while i < len(packed_txs):
            # operation: 1 byte
            operation = packed_txs[i]
            i += 1

            # to: 20 bytes
            to_address = "0x" + packed_txs[i : i + 20].hex()
            i += 20

            # value: 32 bytes
            value = int.from_bytes(packed_txs[i : i + 32], byteorder="big")
            i += 32

            # dataLength: 32 bytes
            data_length = int.from_bytes(packed_txs[i : i + 32], byteorder="big")
            i += 32

            # data: variable
            data = "0x" + packed_txs[i : i + data_length].hex() if data_length > 0 else "0x"
            i += data_length

            transactions.append(
                {
                    "operation": operation,
                    "to": to_address,
                    "value": value,
                    "data": data,
                }
            )

        return transactions


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "MultiSendPayload",
    "MultiSendEncoder",
]
