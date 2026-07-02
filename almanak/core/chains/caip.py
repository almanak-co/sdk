"""CAIP-2 chain-id codec — format / parse for the chain registry.

CAIP-2 (https://chainagnostic.org/CAIPs/caip-2) gives every chain a canonical
string id of the form ``namespace:reference`` (e.g. ``eip155:42161`` for
Arbitrum, ``solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp`` for Solana mainnet).

This module is the formatting/parsing half. Reverse lookup
(``CAIP-2 id → descriptor``) lives on :class:`ChainRegistry`
(``by_caip2`` / ``try_resolve_caip2``) so the index stays next to the rest of
the registry, and ``ChainRegistry.resolve`` accepts CAIP-2 ids directly.

VIB-5175 (CAIP-2/19 adoption, Phase 1). This is a pure string serialization of
data the registry already owns (``chain_id`` + ``family``); chain identity is
unchanged.
"""

from __future__ import annotations

import re

from ._descriptor import ChainDescriptor
from ._registry import ChainRegistry

# CAIP-2 grammar (from the spec):
#   chain_id:  namespace + ":" + reference
#   namespace: [-a-z0-9]{3,8}
#   reference: [-_a-zA-Z0-9]{1,32}
_CAIP2_RE = re.compile(r"^(?P<namespace>[-a-z0-9]{3,8}):(?P<reference>[-_a-zA-Z0-9]{1,32})$")


def to_caip2(chain: str | ChainDescriptor) -> str:
    """Return the CAIP-2 blockchain id for a chain.

    Accepts a chain name / alias / CAIP-2 string or a
    :class:`ChainDescriptor`. Raises ``ValueError`` for an unknown chain —
    same contract as ``ChainRegistry.resolve``.
    """
    if isinstance(chain, ChainDescriptor):
        return chain.caip2
    return ChainRegistry.resolve(str(chain)).caip2


def parse_caip2(value: str) -> tuple[str, str]:
    """Parse a CAIP-2 id into ``(namespace, reference)``.

    Validates against the CAIP-2 grammar and raises ``ValueError`` on a
    malformed id. Does NOT check that the chain is registered — use
    :meth:`ChainRegistry.by_caip2` for that.
    """
    match = _CAIP2_RE.match(value.strip())
    if match is None:
        raise ValueError(f"Malformed CAIP-2 chain id: {value!r} (expected '<namespace>:<reference>', e.g. 'eip155:1')")
    return match.group("namespace"), match.group("reference")


__all__ = ["parse_caip2", "to_caip2"]
