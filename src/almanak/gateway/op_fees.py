"""Pinned OP Stack Fjord/Jovian charges for a retained signed transaction.

These are mined-cost calculations, not enforceable future spending ceilings.
The GasPriceOracle unsigned-transaction API adds signature padding; the mined
calculation instead compresses the actual signed bytes without that padding.
"""

from typing import Literal

from eth_utils import keccak

from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.fastlz import compressed_size
from almanak.models import Block, Model, UInt

GAS_PRICE_ORACLE = "0x420000000000000000000000000000000000000f"
L1_BLOCK = "0x4200000000000000000000000000000000000015"
IMPLEMENTATION_SLOT = hex(int.from_bytes(keccak(text="eip1967.proxy.implementation"), "big") - 1)
PROXY_HASH = "1f958654ab06a152993e7a0ae7b6dbb0d4b19265cc9337b8789fe1353bd9dc35"
IMPLEMENTATION_HASHES = {
    GAS_PRICE_ORACLE: "e9fc7c96c4db0d6078e3d359d7e8c982c350a513cb2c31121adf5e1e8a446614",
    L1_BLOCK: "5f885ca815d2cf27a203123e50b8ae204fdca910b6995d90b2d7700cbb9240d1",
}
L1_BLOCK_REVISIONS = frozenset(
    (
        IMPLEMENTATION_HASHES[L1_BLOCK],
        "a0592f9acd2bafb2b9e003a058bfa3ad20db32ffab1310776a21d2db266bf0da",
        # Base's L1Block 1.8.0 (implementation 0x8bde8f549f56d405f07e1aa15df9e1fc69839881, live from
        # block 53508490, 2026-11-04 15:32 UTC). Its runtime code differs from the allowed 1.7.0
        # (5f885ca8...) in one byte, the embedded version string "1.7.0" -> "1.8.0"; the fee logic
        # is byte-for-byte the same.
        "3828c878b385799d77f7a600108743e04ec8f049bfc778ecd3941c86cf0164bb",
    )
)


class FjordParameters(Model):
    block: Block
    revision: Literal["fjord", "isthmus", "jovian"]
    l1_base_fee: UInt
    blob_base_fee: UInt
    base_fee_scalar: UInt
    blob_base_fee_scalar: UInt
    operator_fee_scalar: UInt
    operator_fee_constant: UInt

    def l1_fee(self, signed_transaction: bytes) -> int:
        size = max(100_000_000, -42_585_600 + 836_500 * compressed_size(signed_transaction))
        price = (
            16 * self.base_fee_scalar * self.l1_base_fee
            + self.blob_base_fee_scalar * self.blob_base_fee
        )
        return size * price // 10**12

    def operator_fee(self, gas_used: int) -> int:
        if gas_used < 0:
            raise ValueError("gas usage cannot be negative")
        if self.revision == "fjord":
            return 0
        if self.revision == "jovian":
            return gas_used * self.operator_fee_scalar * 100 + self.operator_fee_constant
        return gas_used * self.operator_fee_scalar // 10**6 + self.operator_fee_constant


def verify_revision(reader: EthereumReader, block: Block, contract: str) -> None:
    selector = {"blockHash": block.hash, "requireCanonical": True}
    proxy = reader._rpc("eth_getCode", [contract, selector])
    slot = reader._rpc("eth_getStorageAt", [contract, IMPLEMENTATION_SLOT, selector])
    if len(slot) != 66 or int(slot[2:26], 16) != 0:
        raise DataError("unsupported_fee_revision", "invalid OP fee implementation slot")
    code = reader._rpc("eth_getCode", ["0x" + slot[-40:], selector])
    hashes = (keccak(bytes.fromhex(proxy[2:])).hex(), keccak(bytes.fromhex(code[2:])).hex())
    implementations = (
        L1_BLOCK_REVISIONS if contract == L1_BLOCK else (IMPLEMENTATION_HASHES[contract],)
    )
    if hashes[0] != PROXY_HASH or hashes[1] not in implementations:
        raise DataError("unsupported_fee_revision", "unverified OP fee contract revision")


def read_parameters(reader: EthereumReader, block: Block) -> FjordParameters:
    """Fee parameters at `block`, leaving the caller's pinned block unchanged.

    Sign-time revalidation pins a fresh head, reads these parameters at the
    retained quote block and then checks connector revisions at that head. The
    quote-block pin is scoped to these reads so it cannot replace the head.
    """
    with reader.retained_pin():
        return pinned_parameters(reader, block)


def pinned_parameters(reader: EthereumReader, block: Block) -> FjordParameters:
    if reader.pin_block(block.number) != block:
        raise DataError("reorganization", "OP fee parameter block is no longer canonical")
    for contract in IMPLEMENTATION_HASHES:
        verify_revision(reader, block, contract)
    version = reader.call(GAS_PRICE_ORACLE, "version()", block, outputs=("string",))[0]
    flags = tuple(
        reader.call(GAS_PRICE_ORACLE, name, block, outputs=("bool",))[0]
        for name in ("isFjord()", "isIsthmus()", "isJovian()")
    )
    if version != "1.6.0" or flags != (True, True, True):
        raise DataError("unsupported_fee_revision", "OP fee calculation requires verified Jovian")
    fields = {
        "l1_base_fee": "l1BaseFee()",
        "blob_base_fee": "blobBaseFee()",
        "base_fee_scalar": "baseFeeScalar()",
        "blob_base_fee_scalar": "blobBaseFeeScalar()",
    }
    values = {
        field: reader.call(GAS_PRICE_ORACLE, signature, block)[0]
        for field, signature in fields.items()
    }
    return FjordParameters(
        block=block,
        revision="jovian",
        **values,
        operator_fee_scalar=reader.call(L1_BLOCK, "operatorFeeScalar()", block)[0],
        operator_fee_constant=reader.call(L1_BLOCK, "operatorFeeConstant()", block)[0],
    )


# Signed legacy RLP overhead: nonce, gas price, gas limit, to, value, chain-id
# signature triple and list framing never exceed 260 bytes above the calldata.
RLP_OVERHEAD = 260


def exposure_bound(
    reader: EthereumReader, block: Block, gas: int, data_length: int, multiplier_bps: int
) -> int:
    """Quote-time worst case for the L1 data and operator fees of one transaction.

    Admission must bound the total payer cost, not only the L2 execution fee.
    The bound reads the pinned Jovian scalars at the retained quote block, sizes
    the signed transaction from its calldata length (FastLZ level-1 output never
    exceeds the all-literal length, 33/32 of the input plus one), and margins
    the market-driven components by the policy's base-fee multiplier, the same
    drift allowance the L2 quote uses. The operator-fee constant is exact.
    Mined costs are reconciled exactly against the retained receipt identity
    through OpFeeEvidence; this bound governs admission only.
    """
    if gas < 0 or data_length < 0:
        raise ValueError("exposure inputs cannot be negative")
    parameters = read_parameters(reader, block)
    compressed_bound = (33 * (RLP_OVERHEAD + data_length)) // 32 + 1
    size = max(100_000_000, -42_585_600 + 836_500 * compressed_bound)
    price = (
        16 * parameters.base_fee_scalar * parameters.l1_base_fee
        + parameters.blob_base_fee_scalar * parameters.blob_base_fee
    )
    variable = size * price // 10**12 + gas * parameters.operator_fee_scalar * 100
    return (variable * multiplier_bps + 9999) // 10000 + parameters.operator_fee_constant


def fixed_exposure(reader: EthereumReader, block: Block, gas: int, multiplier_bps: int) -> int:
    """The part of `exposure_bound` no market change lowers: its operator-fee terms.

    The operator-fee scalar and constant are chain parameters, so a transaction whose
    bound exceeds `max_transaction_fee` on these terms alone cannot pass by waiting.
    """
    return operator_bound(read_parameters(reader, block), gas, multiplier_bps)


def operator_bound(parameters: FjordParameters, gas: int, multiplier_bps: int) -> int:
    """`exposure_bound`'s operator-fee terms alone, with the same margin and rounding.

    Never above the bound: rounding the margined sum up covers rounding one term up.
    """
    variable = gas * parameters.operator_fee_scalar * 100
    return (variable * multiplier_bps + 9999) // 10000 + parameters.operator_fee_constant
