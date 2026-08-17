"""Typed, JSON-safe token metadata passed from compilers to receipt parsers.

``ActionBundle.metadata`` is a wire payload, so compiler metadata must remain
JSON serializable.  ``TokenMeta`` and ``SwapTokenMeta`` are therefore
``TypedDict`` models rather than runtime dataclasses.  Construction and legacy
payload coercion live here so connectors do not each implement subtly
different precedence, native-token, and decimals rules.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypedDict

from .models import normalize_token_address_for_chain


class TokenMeta(TypedDict):
    """Minimal token metadata needed to scale receipt amounts."""

    address: str
    symbol: str
    decimals: int


class SwapTokenMeta(TypedDict, total=False):
    """Typed input/output token hints for a compiled swap."""

    token_in: TokenMeta
    token_out: TokenMeta


def _read_field(value: object, field: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(field)
    return getattr(value, field, None)


def _coerce_decimals(value: object) -> int | None:
    """Accept integer decimals and legacy integer strings, never lossy values."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        decimals = value
    elif isinstance(value, str):
        try:
            decimals = int(value.strip(), 10)
        except ValueError:
            return None
    else:
        return None
    return decimals if 0 <= decimals <= 77 else None


def _coerce_token_meta(value: object, chain: str) -> TokenMeta | None:
    """Validate one compiler or wire token value.

    Native entries are intentionally omitted. Receipt transfer logs contain
    the wrapped token address, while EVM native currency uses a sentinel whose
    decimals are handled by :func:`resolve_token_decimals`.
    """
    if not value or _read_field(value, "is_native") is True:
        return None

    address = _read_field(value, "address")
    decimals = _read_field(value, "decimals")
    if not isinstance(address, str) or not address.strip() or decimals is None:
        return None

    decimals_int = _coerce_decimals(decimals)
    if decimals_int is None:
        return None

    return TokenMeta(
        address=normalize_token_address_for_chain(address, chain),
        symbol=str(_read_field(value, "symbol") or ""),
        decimals=decimals_int,
    )


def build_swap_token_meta(token_in: object, token_out: object, *, chain: str) -> SwapTokenMeta:
    """Build the canonical JSON-safe swap metadata at compile time."""
    result: SwapTokenMeta = {}
    token_in_meta = _coerce_token_meta(token_in, chain)
    token_out_meta = _coerce_token_meta(token_out, chain)
    if token_in_meta is not None:
        result["token_in"] = token_in_meta
    if token_out_meta is not None:
        result["token_out"] = token_out_meta
    return result


def parse_swap_token_meta(bundle_metadata: Mapping[str, Any], *, chain: str) -> SwapTokenMeta:
    """Read canonical swap metadata, with one compatibility path for old bundles.

    New bundles carry ``swap_token_meta`` built by the compiler. Historical
    bundles only have ``from_token`` and ``to_token``; those are normalized
    here rather than independently in every receipt parser.
    """
    canonical = bundle_metadata.get("swap_token_meta")
    if isinstance(canonical, Mapping):
        return build_swap_token_meta(canonical.get("token_in"), canonical.get("token_out"), chain=chain)
    return build_swap_token_meta(
        bundle_metadata.get("from_token"),
        bundle_metadata.get("to_token"),
        chain=chain,
    )


def build_swap_token_meta_extract_kwargs(
    *,
    field: str,
    bundle_metadata: Mapping[str, Any],
    chain: str,
) -> dict[str, Any]:
    """Build the shared ResultEnricher kwargs for swap receipt parsers."""
    if field != "swap_amounts":
        return {}
    metadata = parse_swap_token_meta(bundle_metadata, chain=chain)
    return {"swap_token_meta": metadata} if metadata else {}


def build_token_meta_hint_map(swap_token_meta: Mapping[str, Any] | None) -> dict[str, tuple[str, int]]:
    """Index validated swap metadata by address for receipt-side lookup."""
    hints: dict[str, tuple[str, int]] = {}
    if not swap_token_meta:
        return hints
    for slot in ("token_in", "token_out"):
        entry = swap_token_meta.get(slot)
        if not isinstance(entry, Mapping):
            continue
        address = entry.get("address")
        decimals = entry.get("decimals")
        if not isinstance(address, str) or not address or decimals is None:
            continue
        decimals_int = _coerce_decimals(decimals)
        if decimals_int is not None:
            hints[address] = (str(entry.get("symbol") or ""), decimals_int)
    return hints


__all__ = [
    "SwapTokenMeta",
    "TokenMeta",
    "build_swap_token_meta",
    "build_swap_token_meta_extract_kwargs",
    "build_token_meta_hint_map",
    "parse_swap_token_meta",
]
