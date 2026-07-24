"""Behavioral tests for the flash-loan calldata builders in compiler_adapters.

Covers ``AaveV3Adapter.get_flash_loan_calldata`` (multi-asset flashLoan) and
``BalancerAdapter.get_flash_loan_calldata`` (Vault flashLoan) by decoding the
hand-rolled ABI encoding with ``eth_abi`` and asserting an exact round-trip:
selector, head offsets, dynamic array tails, and the params-padding branches
(empty / non-32-boundary / exact-32-boundary payloads).
"""

from __future__ import annotations

import pytest
from eth_abi import decode

from almanak.framework.intents.compiler_adapters import AaveV3Adapter, BalancerAdapter
from almanak.framework.intents.compiler_constants import (
    AAVE_FLASH_LOAN_SELECTOR,
    BALANCER_FLASH_LOAN_SELECTOR,
)

RECEIVER = "0x" + "11" * 20
ON_BEHALF_OF = "0x" + "22" * 20
ASSET_A = "0x" + "aa" * 20
ASSET_B = "0x" + "bb" * 20
ASSET_C = "0x" + "cc" * 20

AAVE_FLASH_LOAN_TYPES = [
    "address",  # receiverAddress
    "address[]",  # assets
    "uint256[]",  # amounts
    "uint256[]",  # modes
    "address",  # onBehalfOf
    "bytes",  # params
    "uint16",  # referralCode
]

BALANCER_FLASH_LOAN_TYPES = [
    "address",  # recipient
    "address[]",  # tokens
    "uint256[]",  # amounts
    "bytes",  # userData
]


def _decode_aave(calldata: bytes) -> tuple:
    assert calldata[:4] == bytes.fromhex(AAVE_FLASH_LOAN_SELECTOR[2:])
    return decode(AAVE_FLASH_LOAN_TYPES, calldata[4:])


def _decode_balancer(calldata: bytes) -> tuple:
    assert calldata[:4] == bytes.fromhex(BALANCER_FLASH_LOAN_SELECTOR[2:])
    return decode(BALANCER_FLASH_LOAN_TYPES, calldata[4:])


class TestAaveV3FlashLoanCalldata:
    def setup_method(self) -> None:
        self.adapter = AaveV3Adapter(chain="ethereum")

    def test_single_asset_no_params_round_trips(self) -> None:
        calldata = self.adapter.get_flash_loan_calldata(
            receiver_address=RECEIVER,
            assets=[ASSET_A],
            amounts=[10**18],
            modes=[0],
            on_behalf_of=ON_BEHALF_OF,
        )

        receiver, assets, amounts, modes, on_behalf, params, referral = _decode_aave(calldata)
        assert receiver.lower() == RECEIVER.lower()
        assert [a.lower() for a in assets] == [ASSET_A.lower()]
        assert list(amounts) == [10**18]
        assert list(modes) == [0]
        assert on_behalf.lower() == ON_BEHALF_OF.lower()
        assert params == b""
        assert referral == 0

    def test_multi_asset_mixed_modes_with_unaligned_params(self) -> None:
        """Three assets + a 5-byte params payload exercises the padding branch."""
        calldata = self.adapter.get_flash_loan_calldata(
            receiver_address=RECEIVER,
            assets=[ASSET_A, ASSET_B, ASSET_C],
            amounts=[1, 2**128, 3],
            modes=[0, 1, 2],
            on_behalf_of=ON_BEHALF_OF,
            params=b"hello",
        )

        receiver, assets, amounts, modes, on_behalf, params, referral = _decode_aave(calldata)
        assert [a.lower() for a in assets] == [ASSET_A.lower(), ASSET_B.lower(), ASSET_C.lower()]
        assert list(amounts) == [1, 2**128, 3]
        assert list(modes) == [0, 1, 2]
        assert params == b"hello"
        assert referral == 0

    def test_params_exactly_32_bytes_needs_no_padding(self) -> None:
        payload = b"\x01" * 32
        calldata = self.adapter.get_flash_loan_calldata(
            receiver_address=RECEIVER,
            assets=[ASSET_A],
            amounts=[7],
            modes=[2],
            on_behalf_of=ON_BEHALF_OF,
            params=payload,
        )

        *_, params, referral = _decode_aave(calldata)
        assert params == payload
        assert referral == 0
        # selector + head(7*32) + assets(2*32) + amounts(2*32) + modes(2*32)
        # + params len(32) + params data(32)
        assert len(calldata) == 4 + 7 * 32 + 3 * (2 * 32) + 32 + 32

    def test_empty_asset_lists_encode_empty_arrays(self) -> None:
        calldata = self.adapter.get_flash_loan_calldata(
            receiver_address=RECEIVER,
            assets=[],
            amounts=[],
            modes=[],
            on_behalf_of=ON_BEHALF_OF,
        )

        _, assets, amounts, modes, _, params, _ = _decode_aave(calldata)
        assert list(assets) == []
        assert list(amounts) == []
        assert list(modes) == []
        assert params == b""


class TestBalancerFlashLoanCalldata:
    def setup_method(self) -> None:
        self.adapter = BalancerAdapter(chain="ethereum")

    def test_multi_token_no_user_data_round_trips(self) -> None:
        calldata = self.adapter.get_flash_loan_calldata(
            recipient=RECEIVER,
            tokens=[ASSET_A, ASSET_B],
            amounts=[5, 6],
        )

        recipient, tokens, amounts, user_data = _decode_balancer(calldata)
        assert recipient.lower() == RECEIVER.lower()
        assert [t.lower() for t in tokens] == [ASSET_A.lower(), ASSET_B.lower()]
        assert list(amounts) == [5, 6]
        assert user_data == b""

    def test_user_data_padding_branch_round_trips(self) -> None:
        calldata = self.adapter.get_flash_loan_calldata(
            recipient=RECEIVER,
            tokens=[ASSET_A],
            amounts=[10**6],
            user_data=b"xyz",
        )

        _, tokens, amounts, user_data = _decode_balancer(calldata)
        assert [t.lower() for t in tokens] == [ASSET_A.lower()]
        assert list(amounts) == [10**6]
        assert user_data == b"xyz"

    def test_length_mismatch_raises(self) -> None:
        with pytest.raises(ValueError, match="same length"):
            self.adapter.get_flash_loan_calldata(
                recipient=RECEIVER,
                tokens=[ASSET_A, ASSET_B],
                amounts=[1],
            )

    def test_simple_wrapper_delegates_to_multi(self) -> None:
        direct = self.adapter.get_flash_loan_calldata(
            recipient=RECEIVER,
            tokens=[ASSET_A],
            amounts=[42],
            user_data=b"d",
        )
        via_simple = self.adapter.get_flash_loan_simple_calldata(
            recipient=RECEIVER,
            token=ASSET_A,
            amount=42,
            user_data=b"d",
        )
        assert via_simple == direct
