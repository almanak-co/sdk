"""Verified immutable beacon dispatch and raw balance layout for Robinhood Stock."""

from eth_utils import keccak

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity

PROXY_HASH = "0x6c1fdd40002dcb440c7fff6a84171404d279ccb057803b65826f7546acd65630"
BEACON = "0xe10b6f6b275de231345c20d14ab812db62151b00"
BEACON_HASH = "0x8b465c0b53a2ba499566e9b4ca67d8c90ed6131743df806a570d156956a7e90e"
STOCK_HASH = "0xdc07e86ee482f99641bdafb9a0d772846b167401e094d90a666b94dbdcd1eec7"
BEACON_SLOT = int.from_bytes(keccak(text="eip1967.proxy.beacon"), "big") - 1
BALANCES_NAMESPACE = 0x52C63247E1F47DB19D5CE0460030C497F067CA4CEBF71BA98EEADABE20BACE00


def identity(reader, address, code_hash):
    if reader.reader.chain.chain_id != ChainId("eip155:4663") or code_hash != PROXY_HASH:
        raise DataError("authority", "Stock proxy differs from its verified Robinhood scope")
    # Runtime dispatch uses an immutable, not this slot. The exact full proxy
    # hash binds that immutable; the constructor's stored copy must still agree.
    if reader.storage(address, BEACON_SLOT) != int(BEACON, 16):
        raise DataError("authority", "Stock beacon storage differs from its immutable dispatch")
    if "0x" + keccak(reader.code(BEACON)).hex() != BEACON_HASH:
        raise DataError("authority", "Stock beacon differs from its verified registry revision")
    (implementation,) = reader.call(BEACON, "implementation()")
    implementation_hash = "0x" + keccak(reader.code(implementation)).hex()
    # This exact implementation also embeds BEACON for transfer/approval checks.
    if implementation_hash != STOCK_HASH:
        raise DataError("authority", "Stock implementation differs from its verified revision")
    return ContractIdentity(
        address=address,
        code_hash=code_hash,
        implementation=implementation,
        implementation_hash=implementation_hash,
        beacon=BEACON,
        beacon_hash=BEACON_HASH,
    )
