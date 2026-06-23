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

import re
from dataclasses import dataclass

from almanak.core.chains import ChainRegistry, parse_caip2
from almanak.core.enums import ChainFamily

from .defaults import NATIVE_SENTINEL
from .models import TokenRef, normalize_token_address_for_chain

# Fungible-asset CAIP-19 namespace per execution family. Native assets use the
# chain-agnostic ``slip44`` namespace instead (handled separately below).
_ASSET_NAMESPACE_BY_FAMILY: dict[ChainFamily, str] = {
    ChainFamily.EVM: "erc20",
    ChainFamily.SOLANA: "token",  # SPL token
}

# CAIP-19 grammar (from the spec):
#   asset_namespace: [-a-z0-9]{3,8}
#   asset_reference: [-.%a-zA-Z0-9]{1,128}
_ASSET_NAMESPACE_RE = re.compile(r"^[-a-z0-9]{3,8}$")
_ASSET_REFERENCE_RE = re.compile(r"^[-.%a-zA-Z0-9]{1,128}$")


@dataclass(frozen=True)
class ParsedAsset:
    """The three components of a parsed CAIP-19 asset id.

    ``caip2`` is the chain id (e.g. ``"eip155:1"``); ``asset_namespace`` is
    ``"erc20"`` / ``"slip44"`` / ``"token"`` / …; ``asset_reference`` is the
    address, SLIP-44 coin type, or mint. Pure data — no resolution.
    """

    caip2: str
    asset_namespace: str
    asset_reference: str


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
    chain_enum, address = ref.identity_key
    descriptor = ChainRegistry.get(chain_enum)
    caip2 = descriptor.caip2

    native_sentinel = normalize_token_address_for_chain(NATIVE_SENTINEL, chain_enum)
    if address == native_sentinel:
        slip44 = descriptor.native.slip44
        if slip44 is None:
            raise ValueError(
                f"Cannot emit a CAIP-19 native asset id for {descriptor.name!r}: "
                f"no SLIP-44 coin type registered on its NativeToken. Populate "
                f"NativeToken.slip44 from the SLIP-44 registry to enable this."
            )
        return f"{caip2}/slip44:{slip44}"

    asset_namespace = _ASSET_NAMESPACE_BY_FAMILY[descriptor.family]
    return f"{caip2}/{asset_namespace}:{address}"


def parse_caip19(value: str) -> ParsedAsset:
    """Parse a CAIP-19 asset id into its components.

    Validates the chain part against the CAIP-2 grammar and the asset part
    against the CAIP-19 grammar; raises ``ValueError`` on a malformed id. Does
    NOT resolve the chain or token — use ``TokenResolver.resolve_caip19`` for a
    fully-resolved token. The optional NFT ``token_id`` segment is not
    supported in Phase 1 and is rejected.
    """
    chain_part, sep, asset_part = value.strip().partition("/")
    if not sep:
        raise ValueError(
            f"Malformed CAIP-19 asset id: {value!r} (expected '<caip2>/<asset_namespace>:<asset_reference>')"
        )
    # Chain part must be a well-formed CAIP-2 id. Re-frame the CAIP-2 error as a
    # CAIP-19 one so callers get a single, consistent error surface.
    try:
        parse_caip2(chain_part)
    except ValueError as exc:
        raise ValueError(f"Malformed CAIP-19 asset id: {value!r} (invalid CAIP-2 chain part {chain_part!r})") from exc

    asset_namespace, ns_sep, asset_reference = asset_part.partition(":")
    if not ns_sep or not _ASSET_NAMESPACE_RE.match(asset_namespace) or not _ASSET_REFERENCE_RE.match(asset_reference):
        raise ValueError(
            f"Malformed CAIP-19 asset id: {value!r} "
            f"(expected '<caip2>/<asset_namespace>:<asset_reference>', "
            f"e.g. 'eip155:1/erc20:0x6b17…1d0f')"
        )
    return ParsedAsset(caip2=chain_part, asset_namespace=asset_namespace, asset_reference=asset_reference)


__all__ = ["ParsedAsset", "parse_caip19", "token_ref_to_caip19"]
