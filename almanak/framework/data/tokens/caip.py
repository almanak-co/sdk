"""CAIP-19 asset-id codec — the canonical string form of :class:`TokenRef`.

CAIP-19 (https://chainagnostic.org/CAIPs/caip-19) identifies an asset as
``<caip2>/<asset_namespace>:<asset_reference>`` — e.g.
``eip155:1/erc20:0x6b175474e89094c44da98b954eedeac495271d0f`` for DAI,
``eip155:1/slip44:60`` for native ETH, or
``solana:5eykt4UsFv8P8…/token:<mint>`` for an SPL token.

This is exactly the identity ``TokenRef`` already carries — ``(chain, address)``
plus an "is this the native asset?" check — rendered as a string. The reverse
path (CAIP-19 → fully-resolved token *with decimals*) lives on the
``TokenResolver`` because CAIP-19 encodes identity only, not decimals.

VIB-5175 (CAIP-2/19 adoption, Phase 1). Lands under the ADR-002 / VIB-5175
epic as additive serialization — token identity (frozen ``(chain, address)``)
is unchanged.
"""

from __future__ import annotations

from almanak.core.asset_identity import ParsedAsset, parse_caip19

from .models import TokenRef


def token_ref_to_caip19(ref: TokenRef) -> str:
    """Render a :class:`TokenRef` identity as a CAIP-19 asset id.

    Native tokens (address == the chain's native sentinel) emit
    ``<caip2>/slip44:<coin_type>``; fungible tokens emit
    ``<caip2>/erc20:<address>`` (EVM) or ``<caip2>/token:<address>``
    (Solana SPL). The address case follows ``TokenRef`` normalization (EVM
    lowercase, Solana base58 preserved).

    Raises:
        ValueError: the token is native but the chain has no registered
            SLIP-44 coin type (``NativeToken.slip44 is None``) — we fail loudly
            rather than emit a non-standard native id.
    """
    return ref.asset_identity.caip19


__all__ = ["ParsedAsset", "parse_caip19", "token_ref_to_caip19"]
