"""CAIP-19 identity of HyperCore spot tokens.

A HyperCore account is the same address on `eip155:999`, and a spot token is
identified by the HyperEVM asset `spotMeta` links it to. Two tokens need more than
the link, so both are pinned by the HyperCore token ID the venue assigned them,
never by name:

- USDC (token 0) is linked to Circle's CoreDepositWallet, the only way USDC enters
  HyperCore, not to the USDC ERC-20. Its identity is the wallet's `token()`, read at
  block 49023365 in the phase 0 report and pinned here with the wallet.
- HYPE is linked to nothing: it is HyperEVM's native asset, which crosses through the
  system address 0x2222...2222. Its identity is the chain's native asset.

Any other token with no linked contract has no HyperEVM identity.
"""

from almanak.caip import AssetType
from almanak.chains import HYPEREVM
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_shapes import SpotToken

CORE_DEPOSIT_WALLET = "0x6b9e773128f453f5c2c60935ee2de2cbc5390a24"
USDC = AssetType("eip155:999/erc20:0xb88339cb7199b77e23db6e890353e22632ba630f")
USDC_TOKEN_ID = "0x6d1e7cde53ba9467b783cb7c530ce054"
HYPE_TOKEN_ID = "0x0d01dc56dcaaca66ad901c959b4011ec"


def spot_asset(token: SpotToken) -> AssetType | None:
    """The token's HyperEVM asset, or None when HyperCore links it to none."""
    linked = None if token.evmContract is None else token.evmContract.address
    if token.tokenId == USDC_TOKEN_ID:
        if linked != CORE_DEPOSIT_WALLET:
            raise DataError("identity", "HyperCore USDC is not linked to the reviewed wallet")
        return USDC
    if token.tokenId == HYPE_TOKEN_ID:
        if linked is not None:
            raise DataError("identity", "HyperCore HYPE is linked to a contract; review it")
        return HYPEREVM.native_asset
    if linked is None:
        return None
    if linked == CORE_DEPOSIT_WALLET:
        raise DataError("identity", f"HyperCore token {token.tokenId} links the USDC wallet")
    return AssetType(f"{HYPEREVM.chain}/erc20:{linked}")
