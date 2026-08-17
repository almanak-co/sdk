"""Bounded generated seed used only by offline permission discovery.

The artifact contains one representative market per supported chain. It exists
solely so Safe/Zodiac discovery can compile real GMX calldata without network
access. A seed row is not current-listing evidence and must never enter
``market_catalog``. Regenerate it with
``scripts/gmx_v2/generate_permission_seed.py``; the audit test re-verifies each
row against the GMX catalogue and ``Reader.getMarket``.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Any

from eth_utils import to_checksum_address

from .addresses import GMX_V2
from .market_identity import canonicalise_market
from .market_metadata import ResolvedGmxMarket

_SEED_PATH = Path(__file__).with_name("permission_seed.json")
_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_RECORD_FIELDS = frozenset(ResolvedGmxMarket.__dataclass_fields__)


def _record(chain: str, raw: Any) -> ResolvedGmxMarket:
    if not isinstance(raw, dict) or set(raw) != _RECORD_FIELDS:
        raise RuntimeError(f"GMX permission seed row {chain!r} must have exactly {sorted(_RECORD_FIELDS)}")
    values = dict(raw)
    for field in ("market_token", "index_token", "long_token", "short_token"):
        value = values[field]
        if not isinstance(value, str) or not _ADDRESS_RE.fullmatch(value):
            raise RuntimeError(f"GMX permission seed {chain}.{field} is not an EVM address: {value!r}")
        values[field] = to_checksum_address(value)
    for field in ("index_token_decimals", "long_token_decimals", "short_token_decimals"):
        decimals = values[field]
        if isinstance(decimals, bool) or not isinstance(decimals, int) or not 0 <= decimals <= 30:
            raise RuntimeError(f"GMX permission seed {chain} has invalid {field}: {decimals!r}")
    for field in ("label", "index_symbol", "long_token_symbol", "short_token_symbol"):
        if not isinstance(values[field], str) or not values[field].strip():
            raise RuntimeError(f"GMX permission seed {chain}.{field} must be non-empty")
    return ResolvedGmxMarket(**values)


@lru_cache(maxsize=1)
def permission_markets() -> Mapping[str, ResolvedGmxMarket]:
    """Load and validate the one-row-per-chain generated artifact."""
    payload = json.loads(_SEED_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {"schema_version", "markets"}:
        raise RuntimeError("GMX permission seed has an invalid top-level shape")
    if payload["schema_version"] != 2 or not isinstance(payload["markets"], dict):
        raise RuntimeError("GMX permission seed schema_version must be 2")
    expected_chains = set(GMX_V2)
    if set(payload["markets"]) != expected_chains:
        raise RuntimeError(
            "GMX permission seed must contain exactly one row for every supported chain: "
            f"expected={sorted(expected_chains)} actual={sorted(payload['markets'])}"
        )
    return MappingProxyType({chain: _record(chain, payload["markets"][chain]) for chain in sorted(expected_chains)})


def permission_market(chain: str, market: str) -> ResolvedGmxMarket | None:
    """Resolve a permission-only label/address without publishing it as verified."""
    record = permission_markets().get(chain.lower())
    if record is None:
        return None
    query = canonicalise_market(market)
    if query.lower() in {record.label.lower(), record.market_token.lower()}:
        return record
    return None


__all__ = ["permission_market", "permission_markets"]
