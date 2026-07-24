"""Unit tests for MultiSendEncoder encoding paths.

Covers `MultiSendEncoder.encode_transactions` and
`MultiSendEncoder.encode_from_dicts` in
almanak/framework/execution/signer/safe/multisend.py:

- empty-list and missing-'to' validation errors
- data normalization branches: "0x"-prefixed str, bare-hex str, empty str,
  raw bytes, and falsy non-str (None)
- value normalization branches (dicts): missing, int, "0x" hex str, decimal str
- packed byte layout (operation + to + value + dataLength + data)
- round-trip via decode_multisend_data
- equivalence between the UnsignedTransaction and dict encoding paths
"""

from __future__ import annotations

import pytest
from eth_abi import decode
from eth_utils import to_checksum_address
from web3 import Web3

from almanak.framework.execution.interfaces import UnsignedTransaction
from almanak.framework.execution.signer.safe.constants import (
    MULTISEND_SELECTOR,
    SafeOperation,
    get_multisend_address,
)
from almanak.framework.execution.signer.safe.multisend import (
    MultiSendEncoder,
    MultiSendPayload,
)

WEB3 = Web3()

ADDR_A = to_checksum_address("0x" + "11" * 20)
ADDR_B = to_checksum_address("0x" + "22" * 20)
ADDR_C = to_checksum_address("0x" + "33" * 20)

SELECTOR_BYTES = bytes.fromhex(MULTISEND_SELECTOR[2:])


def make_tx(
    to: str | None = ADDR_A,
    value: int = 0,
    data: object = "0x",
) -> UnsignedTransaction:
    """Build a minimal EIP-1559 UnsignedTransaction for encoding tests."""
    return UnsignedTransaction(
        to=to,
        value=value,
        data=data,  # type: ignore[arg-type]  # bytes/None exercised on purpose
        chain_id=42161,
        gas_limit=100_000,
        max_fee_per_gas=1_000_000_000,
        max_priority_fee_per_gas=1_000_000,
    )


def unpack(calldata: bytes) -> bytes:
    """Strip the multiSend selector and ABI wrapping, return packed txs."""
    assert calldata[:4] == SELECTOR_BYTES
    (packed,) = decode(["bytes"], calldata[4:])
    return packed


# =============================================================================
# encode_transactions
# =============================================================================


class TestEncodeTransactions:
    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="empty transaction list"):
            MultiSendEncoder.encode_transactions([], WEB3)

    def test_missing_to_raises_with_index(self):
        txs = [make_tx(to=ADDR_A, data="0x01"), make_tx(to=None)]
        with pytest.raises(ValueError, match="Transaction 1 has no 'to' address"):
            MultiSendEncoder.encode_transactions(txs, WEB3)

    def test_single_tx_round_trip(self):
        tx = make_tx(to=ADDR_A, value=123, data="0xdeadbeef")
        calldata = MultiSendEncoder.encode_transactions([tx], WEB3)

        decoded = MultiSendEncoder.decode_multisend_data(calldata)
        assert decoded == [
            {
                "operation": 0,
                "to": ADDR_A.lower(),
                "value": 123,
                "data": "0xdeadbeef",
            }
        ]

    def test_packed_byte_layout(self):
        tx = make_tx(to=ADDR_B, value=7, data="0xa1b2c3")
        packed = unpack(MultiSendEncoder.encode_transactions([tx], WEB3))

        data_bytes = bytes.fromhex("a1b2c3")
        assert len(packed) == 85 + len(data_bytes)
        assert packed[0] == 0  # operation = CALL
        assert packed[1:21] == bytes.fromhex(ADDR_B[2:])
        assert int.from_bytes(packed[21:53], "big") == 7
        assert int.from_bytes(packed[53:85], "big") == len(data_bytes)
        assert packed[85:] == data_bytes

    def test_data_variant_branches(self):
        """Each data-normalization branch: 0x str, bare str, empty str, bytes, None."""
        txs = [
            make_tx(data="0xdeadbeef"),  # str with 0x prefix
            make_tx(data="cafebabe"),  # bare-hex str
            make_tx(data=""),  # empty str -> b""
            make_tx(data=b"\x01\x02"),  # raw bytes
            make_tx(data=None),  # falsy non-str -> b""
        ]
        calldata = MultiSendEncoder.encode_transactions(txs, WEB3)
        decoded = MultiSendEncoder.decode_multisend_data(calldata)

        assert [d["data"] for d in decoded] == [
            "0xdeadbeef",
            "0xcafebabe",
            "0x",
            "0x0102",
            "0x",
        ]
        # Packed size = sum of (85 + len(data)) per tx
        packed = unpack(calldata)
        assert len(packed) == 5 * 85 + 4 + 4 + 0 + 2 + 0

    def test_lowercase_address_is_checksummed(self):
        tx = make_tx(to=ADDR_C.lower(), data="0x")
        decoded = MultiSendEncoder.decode_multisend_data(MultiSendEncoder.encode_transactions([tx], WEB3))
        assert decoded[0]["to"] == ADDR_C.lower()


# =============================================================================
# encode_from_dicts
# =============================================================================


class TestEncodeFromDicts:
    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="empty transaction list"):
            MultiSendEncoder.encode_from_dicts([], WEB3)

    def test_missing_to_key_raises(self):
        with pytest.raises(ValueError, match="Transaction 0 missing 'to' address"):
            MultiSendEncoder.encode_from_dicts([{"value": 1, "data": "0x"}], WEB3)

    def test_none_to_raises_with_index(self):
        txs = [{"to": ADDR_A, "data": "0x"}, {"to": None, "data": "0x"}]
        with pytest.raises(ValueError, match="Transaction 1 missing 'to' address"):
            MultiSendEncoder.encode_from_dicts(txs, WEB3)

    def test_value_variant_branches(self):
        """Value branches: missing (default 0), int, 0x-hex str, decimal str."""
        txs = [
            {"to": ADDR_A},  # value missing -> 0
            {"to": ADDR_A, "value": 5},  # int
            {"to": ADDR_A, "value": "0x10"},  # hex str -> 16
            {"to": ADDR_A, "value": "256"},  # decimal str -> 256
        ]
        calldata = MultiSendEncoder.encode_from_dicts(txs, WEB3)
        decoded = MultiSendEncoder.decode_multisend_data(calldata)
        assert [d["value"] for d in decoded] == [0, 5, 16, 256]

    def test_data_variant_branches(self):
        """Data branches: missing (default "0x"), 0x str, bare str, empty str, bytes, None."""
        txs = [
            {"to": ADDR_A},  # missing -> "0x"
            {"to": ADDR_A, "data": "0xdeadbeef"},
            {"to": ADDR_A, "data": "beef"},
            {"to": ADDR_A, "data": ""},
            {"to": ADDR_A, "data": b"\x99"},
            {"to": ADDR_A, "data": None},  # falsy non-str -> b""
        ]
        calldata = MultiSendEncoder.encode_from_dicts(txs, WEB3)
        decoded = MultiSendEncoder.decode_multisend_data(calldata)
        assert [d["data"] for d in decoded] == [
            "0x",
            "0xdeadbeef",
            "0xbeef",
            "0x",
            "0x99",
            "0x",
        ]

    def test_matches_encode_transactions_output(self):
        """Both encoding paths produce identical calldata for equivalent inputs."""
        txs_obj = [
            make_tx(to=ADDR_A, value=42, data="0xdeadbeef"),
            make_tx(to=ADDR_B, value=0, data="0x"),
        ]
        txs_dict = [
            {"to": ADDR_A, "value": 42, "data": "0xdeadbeef"},
            {"to": ADDR_B, "value": 0, "data": "0x"},
        ]
        assert MultiSendEncoder.encode_from_dicts(txs_dict, WEB3) == MultiSendEncoder.encode_transactions(txs_obj, WEB3)


# =============================================================================
# build_payload (thin wrapper over encode_transactions)
# =============================================================================


class TestBuildPayload:
    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="empty transaction list"):
            MultiSendEncoder.build_payload([], "arbitrum", WEB3)

    def test_payload_shape(self):
        tx = make_tx(to=ADDR_A, value=1, data="0x01")
        payload = MultiSendEncoder.build_payload([tx], "arbitrum", WEB3)

        assert isinstance(payload, MultiSendPayload)
        assert payload.to == WEB3.to_checksum_address(get_multisend_address("arbitrum"))
        assert payload.value == 0
        assert payload.operation == SafeOperation.DELEGATE_CALL
        assert payload.data.startswith(MULTISEND_SELECTOR)
        # Payload data round-trips to the original tx
        decoded = MultiSendEncoder.decode_multisend_data(payload.data)
        assert decoded[0]["to"] == ADDR_A.lower()
        assert decoded[0]["value"] == 1


# =============================================================================
# decode_multisend_data error branches
# =============================================================================


class TestDecodeMultisendData:
    def test_accepts_hex_string_input(self):
        tx = make_tx(to=ADDR_A, value=9, data="0xff")
        calldata = MultiSendEncoder.encode_transactions([tx], WEB3)
        decoded = MultiSendEncoder.decode_multisend_data("0x" + calldata.hex())
        assert decoded[0] == {"operation": 0, "to": ADDR_A.lower(), "value": 9, "data": "0xff"}

    def test_accepts_bare_hex_string_input(self):
        tx = make_tx(to=ADDR_A, value=0, data="0x")
        calldata = MultiSendEncoder.encode_transactions([tx], WEB3)
        decoded = MultiSendEncoder.decode_multisend_data(calldata.hex())
        assert decoded[0]["to"] == ADDR_A.lower()

    def test_too_short_calldata_raises(self):
        with pytest.raises(ValueError, match="too short"):
            MultiSendEncoder.decode_multisend_data(b"\x8d\x80")

    def test_wrong_selector_raises(self):
        bogus = b"\xde\xad\xbe\xef" + b"\x00" * 64
        with pytest.raises(ValueError, match="Invalid selector"):
            MultiSendEncoder.decode_multisend_data(bogus)
