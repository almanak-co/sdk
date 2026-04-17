"""Default token registry — loads from JSON, exposes legacy module API.

The token data lives in sibling JSON files under ``data/``:

* ``tokens.json`` — every registered token (symbol, addresses, decimals,
  coingecko_id, bridged overrides, ``var_name`` for backward-compat imports,
  ``in_default_set`` toggle). **Source of truth.**
* ``chains.json`` — per-chain config (wrapped native address, native sentinel).
  **Source of truth.**
* ``symbol_aliases.json`` — bridged token aliases (USDC.e, USDbC, ...).
  **Source of truth.**
* ``stablecoins.json`` — *snapshot only*. The canonical ``STABLECOINS`` set
  is re-exported from ``almanak.core.constants`` (see the import below) —
  edit that file to add a stablecoin, not the JSON. The JSON is kept for
  external tooling that can't import core.

This module reads those files at import time and rebuilds the module-level
variables every legacy caller expects: ``USDC``, ``WETH``, ``DEFAULT_TOKENS``,
``WRAPPED_NATIVE``, ``SYMBOL_ALIASES``, ``STABLECOINS``, ``NATIVE_SENTINEL``,
``get_coingecko_id``, ``get_coingecko_ids``. No call sites change.

Schema: ``Token.from_dict`` plus two extra fields per record:

* ``var_name`` — the original Python identifier (e.g. ``USDC_E_ARBITRUM``).
  Used for backward-compat imports.
* ``in_default_set`` — whether this token belongs in ``DEFAULT_TOKENS``
  (some Solana bridged variants are kept importable but excluded to avoid
  duplicate addresses).

See ``blueprints/17-token-resolution.md`` for the full design.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# STABLECOINS is re-exported from core/constants for backwards compatibility.
# The JSON copy is a snapshot; the canonical definition lives in core.
from almanak.core.constants import STABLECOINS as STABLECOINS  # noqa: F401

from .models import BridgeType, ChainTokenConfig, Token

_DATA_DIR = Path(__file__).parent / "data"


def _load_json(name: str) -> Any:
    path = _DATA_DIR / name
    return json.loads(path.read_text(encoding="utf-8"))


# -----------------------------------------------------------------------------
# Chain-level config
# -----------------------------------------------------------------------------

_chains_blob = _load_json("chains.json")
_chains = _chains_blob["chains"]

# The EVM native sentinel is a protocol-level convention, not per-chain data:
# many DEXes / routers use this address to denote "native gas token" rather
# than a wrapped ERC-20. We hard-code it here so its correctness can't be
# broken by a chains.json reorder or a single chain file missing the
# ``native_sentinel`` field. ``TokenResolver._token_to_resolved`` uses this
# constant to set ``is_native``; getting it wrong would silently break every
# native-token swap / balance call.
NATIVE_SENTINEL: str = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

# Cross-check: every EVM chain in chains.json must agree. (Solana doesn't use
# the EVM sentinel; its entry carries the WSOL mint in ``native_sentinel``.)
_unexpected_sentinels = {
    chain: cfg.get("native_sentinel")
    for chain, cfg in _chains.items()
    if chain != "solana" and cfg.get("native_sentinel") and cfg["native_sentinel"] != NATIVE_SENTINEL
}
if _unexpected_sentinels:
    raise RuntimeError(
        f"chains.json has EVM chains with non-standard native_sentinel: "
        f"{_unexpected_sentinels!r}. Expected {NATIVE_SENTINEL!r} for all EVM chains."
    )

WRAPPED_NATIVE: dict[str, str] = {chain: cfg["wrapped_native_address"] for chain, cfg in _chains.items()}


# -----------------------------------------------------------------------------
# Symbol aliases
# -----------------------------------------------------------------------------

_aliases_blob = _load_json("symbol_aliases.json")
# The upstream VIB-2950 token-resolution overhaul moved SYMBOL_ALIASES out of this
# Python file and into ``data/symbol_aliases.json``. Polygon's WPOL/POL aliases
# (added in VIB-2971) are maintained there alongside the rest.
SYMBOL_ALIASES: dict[tuple[str, str], str] = {
    (chain, alias): address
    for chain, chain_aliases in _aliases_blob["aliases"].items()
    for alias, address in chain_aliases.items()
}


# -----------------------------------------------------------------------------
# Tokens
# -----------------------------------------------------------------------------

_tokens_blob = _load_json("tokens.json")
_token_records: list[dict[str, Any]] = _tokens_blob["tokens"]


def _record_to_token(record: dict[str, Any]) -> Token:
    """Build a Token from a JSON record (strips the loader-only fields)."""
    payload = {k: v for k, v in record.items() if k not in {"var_name", "in_default_set"}}
    return Token.from_dict(payload)


# Build all tokens and an index by var_name so legacy imports keep working.
_tokens_by_var: dict[str, Token] = {}
_default_tokens_in_order: list[Token] = []

for _record in _token_records:
    _token = _record_to_token(_record)
    _var_name = _record["var_name"]
    if _var_name in _tokens_by_var:
        raise RuntimeError(f"Duplicate var_name {_var_name!r} in tokens.json")
    _tokens_by_var[_var_name] = _token
    if _record.get("in_default_set", True):
        _default_tokens_in_order.append(_token)

DEFAULT_TOKENS: list[Token] = _default_tokens_in_order


# -----------------------------------------------------------------------------
# Explicit module-level declarations for legacy imports referenced inside the
# typechecked ``almanak/`` tree. Everything else (2,500+ long-tail tokens) is
# served via ``__getattr__`` below.
#
# Keep this list to the identifiers that mypy-checked modules actually import
# by name: anything only imported from tests falls under ``ignore_errors`` in
# pyproject.toml so doesn't need to live here.
# -----------------------------------------------------------------------------

# Native / wrapped native used across the framework.
ETH: Token = _tokens_by_var["ETH"]
WETH: Token = _tokens_by_var["WETH"]
WBTC: Token = _tokens_by_var["WBTC"]

# Stablecoins referenced by Solana fork manager and protocol SDKs.
USDC: Token = _tokens_by_var["USDC"]
USDT: Token = _tokens_by_var["USDT"]
DAI: Token = _tokens_by_var["DAI"]

# Solana-specific tokens referenced by Solana fork manager.
SOL: Token = _tokens_by_var["SOL"]
WSOL: Token = _tokens_by_var["WSOL"]
JUP: Token = _tokens_by_var["JUP"]


def __getattr__(name: str) -> Any:
    """Lazy module-level lookup for every ``var_name`` in ``tokens.json``.

    Replaces the earlier ``globals().update(_tokens_by_var)`` pattern —
    same runtime contract (``from ...defaults import USDC`` works), but
    statically analyzable: mypy accepts any attribute access from this
    module (it now always resolves to a ``Token`` at runtime). Return
    type is ``Any`` so callers don't need to cast the result before
    accessing fields like ``.addresses`` / ``.decimals``. PEP 562.
    """
    try:
        return _tokens_by_var[name]
    except KeyError as e:
        raise AttributeError(f"module 'defaults' has no attribute {name!r}") from e


def __dir__() -> list[str]:
    """Advertise every loadable token var alongside the static exports so
    IDEs, ``dir(defaults)`` and ``help(defaults)`` stay useful."""
    return sorted(set(globals().keys()) | set(_tokens_by_var.keys()))


# -----------------------------------------------------------------------------
# CoinGecko helpers (preserved from prior defaults.py)
# -----------------------------------------------------------------------------


def get_coingecko_id(symbol: str) -> str | None:
    """Return the CoinGecko ID for a token symbol, or None if ambiguous / unknown.

    Many symbols map to more than one project (e.g. GMT = GoMining vs STEPN;
    JPYC = two separate JPY Coin projects; EUSD = Electronic Dollar vs eUSD).
    Picking the "first" one is a correctness hazard: it silently routes the
    price oracle to the wrong asset. Instead, return ``None`` when the
    registry contains multiple distinct CoinGecko IDs for the same symbol,
    and let the caller fall back to address-based lookup or other data
    sources.

    For unambiguous symbols this stays fast (single pass, early return).
    """
    symbol_upper = symbol.upper()
    seen: str | None = None
    for token in DEFAULT_TOKENS:
        if token.symbol.upper() != symbol_upper:
            continue
        if not token.coingecko_id:
            continue
        if seen is None:
            seen = token.coingecko_id
        elif seen != token.coingecko_id:
            # Genuine ambiguity; caller should resolve by address.
            return None
    return seen


def get_coingecko_ids() -> dict[str, str]:
    """Return a mapping of token symbol -> CoinGecko ID for every *unambiguous*
    symbol with an ID set. Symbols that map to more than one distinct
    CoinGecko ID are excluded -- see :func:`get_coingecko_id` for why."""
    by_symbol: dict[str, set[str]] = {}
    for token in DEFAULT_TOKENS:
        if not token.coingecko_id:
            continue
        by_symbol.setdefault(token.symbol.upper(), set()).add(token.coingecko_id)
    return {symbol: next(iter(ids)) for symbol, ids in by_symbol.items() if len(ids) == 1}


# -----------------------------------------------------------------------------
# __all__ — keep the legacy export surface. Backward-compat imports rely on
# this module exposing specific token variable names.
# -----------------------------------------------------------------------------

# Built as a literal list first, then extended with the token var names loaded
# from JSON. The starred-expansion form (``*sorted(...)``) works at runtime but
# is flagged by Ruff (PLE0604) and can make ``__all__`` hard to diff.
__all__: list[str] = [
    # Constants
    "NATIVE_SENTINEL",
    "WRAPPED_NATIVE",
    "STABLECOINS",
    "SYMBOL_ALIASES",
    # Helpers
    "DEFAULT_TOKENS",
    "get_coingecko_id",
    "get_coingecko_ids",
    # Models re-exported so callers can `from .defaults import Token, BridgeType`
    "Token",
    "BridgeType",
    "ChainTokenConfig",
]
__all__.extend(sorted(_tokens_by_var.keys()))
