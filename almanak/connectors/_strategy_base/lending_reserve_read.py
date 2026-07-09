"""Connector-owned lending reserve-discovery plans (VIB-4951).

Generalizes the ``list_lending_reserves`` agent tool beyond Aave: each lending
connector publishes a *pure* :class:`LendingReserveDiscoveryPlan` — call
descriptors (to, data, id) plus decode callables — through its agent-read
provider. The executor owns ALL egress (gateway ``eth_call``), the safety cap,
the latency budget, the ``--asset`` filter, and truncation semantics, so every
protocol inherits the exact VIB-4925 tool contract.

Gateway-boundary note: nothing in this module (or in any connector plan
builder) may perform network I/O. A plan is data + pure functions; the
framework executes it. This mirrors ``lending_read_base`` / ``perps_read``.

Two enumeration shapes are supported:

* **Call-enumerated** (Aave-fork): ``enumeration_call`` +
  ``decode_enumeration`` produce the entries; each entry carries its own
  per-reserve ``config_call`` decoded by ``decode_config``.
* **Static-enumerated** (Compound comets, Morpho markets): ``static_entries``
  lists the reserves up front; an entry either embeds a ``static_config``
  (no RPC — e.g. Morpho's immutable LLTV) or a ``config_call`` for live
  reads (e.g. Compound ``getAssetInfoByAddress`` collateral factors).

Beyond the one primary config read, an entry may carry
``supplementary_calls``: additional per-reserve reads decoded by
the plan's ``decode_supplementary`` into extra row fields declared in
``supplementary_fields``. Supplementary reads are strictly fail-open in the
executor — an RPC failure, inner-call revert, or decode mismatch leaves those
fields ``None`` (unmeasured; Empty != Zero) and never sets the row ``error``.
``annotate_row`` lets the plan add protocol-specific ``detail`` notes derived
from the measured row (e.g. Aave's "base LTV zero — eMode-only" risk note)
without the executor learning any protocol semantics.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TypeGuard

from eth_utils import function_signature_to_4byte_selector

__all__ = [
    "LendingReserveDiscoveryPlan",
    "ReserveCall",
    "ReserveConfigRow",
    "ReserveEntry",
    "aave_fork_reserve_discovery_plan",
]


@dataclass(frozen=True)
class ReserveCall:
    """One ``eth_call`` descriptor. ``id`` is the gateway trace id — keep it
    stable per protocol (goldens and log greps key on it)."""

    to: str
    data: str
    id: str


@dataclass(frozen=True)
class ReserveConfigRow:
    """Decoded per-reserve risk config, tool-row shaped. ``None`` = unmeasured
    (Empty != Zero) — the executor serializes ``None`` fields verbatim."""

    borrowing_enabled: bool | None
    usage_as_collateral_enabled: bool | None
    is_active: bool | None
    is_frozen: bool | None
    ltv_bps: int | None
    liquidation_threshold_bps: int | None
    extra: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ReserveEntry:
    """One enumerated reserve: exactly one of ``config_call`` (live read,
    decoded by the plan's ``decode_config``) or ``static_config``.

    ``supplementary_calls`` (optional) are extra per-reserve reads decoded by
    the plan's ``decode_supplementary`` — strictly fail-open in the executor
    (their fields stay ``None`` on any failure; the row ``error`` is reserved
    for the primary config read).
    """

    symbol: str
    address: str
    config_call: ReserveCall | None = None
    static_config: ReserveConfigRow | None = None
    supplementary_calls: tuple[ReserveCall, ...] = ()


@dataclass(frozen=True)
class LendingReserveDiscoveryPlan:
    """Pure reserve-discovery plan for one ``(protocol, chain)``.

    Exactly one of (``enumeration_call`` + ``decode_enumeration``) or
    ``static_entries`` must be set. ``decode_config`` is required when any
    entry carries a ``config_call``.

    Supplementary contract:

    * ``supplementary_fields`` declares the exact universe of extra row keys
      this plan may fill — the executor seeds each as ``None`` on every row
      (uniform shape; ``None`` = unmeasured) and ignores any other key a
      decode returns.
    * ``decode_supplementary(call_id, raw_hex)`` decodes one supplementary
      payload into a partial ``{field: value}`` mapping, dispatching on the
      call's ``id``. It MUST raise ``ValueError`` on any payload it does not
      recognize byte-for-byte (strict length checks) so a mismatched response
      fails open to ``None`` instead of fabricating values.
    * ``annotate_row(row)`` (optional) receives the fully-resolved row dict
      (read-only) and returns extra ``detail`` entries to merge (e.g. a
      ``risk_note``). Return an empty mapping for "no annotation".
    """

    protocol: str
    provider_address: str
    enumeration_call: ReserveCall | None = None
    decode_enumeration: Callable[[str], list[ReserveEntry]] | None = None
    static_entries: tuple[ReserveEntry, ...] | None = None
    decode_config: Callable[[str], ReserveConfigRow] | None = None
    supplementary_fields: tuple[str, ...] = ()
    decode_supplementary: Callable[[str, str], Mapping[str, object]] | None = None
    annotate_row: Callable[[Mapping[str, object]], Mapping[str, str]] | None = None

    def __post_init__(self) -> None:
        has_call = self.enumeration_call is not None
        has_decode = self.decode_enumeration is not None
        has_static = self.static_entries is not None
        if has_call != has_decode:
            raise ValueError(f"{self.protocol}: enumeration_call and decode_enumeration must be set together")
        if has_call == has_static:
            raise ValueError(f"{self.protocol}: plan must be exactly one of call-enumerated or static-enumerated")
        if (len(self.supplementary_fields) > 0) != (self.decode_supplementary is not None):
            raise ValueError(f"{self.protocol}: supplementary_fields and decode_supplementary must be set together")


# Aave V3 AaveProtocolDataProvider supplementary risk-context reads.
# Selectors are derived, never hand-typed (repo convention — see
# multicall.py / gmx_v2/orders_read.py); the derivation is trusted because the
# same helper reproduces the two selectors pinned in aave_helpers
# (getReserveConfigurationData 0x3e150141, getAllReservesTokens 0xb316ff89).
#
#   getReserveCaps(address)          -> (uint256 borrowCap, uint256 supplyCap)  0x46fbe558
#   getReserveEModeCategory(address) -> uint256                                 0x163a0f20
#   getPaused(address)               -> bool                                    0xb55d9904
#
# Caps are reported in WHOLE-TOKEN units (unscaled by decimals), exactly as
# Aave's ReserveConfiguration stores them; a measured 0 means "no cap" in Aave
# semantics and is reported verbatim (Empty != Zero — never translated).
_AAVE_GET_RESERVE_CAPS_SELECTOR = "0x" + function_signature_to_4byte_selector("getReserveCaps(address)").hex()
_AAVE_GET_RESERVE_EMODE_SELECTOR = "0x" + function_signature_to_4byte_selector("getReserveEModeCategory(address)").hex()
_AAVE_GET_PAUSED_SELECTOR = "0x" + function_signature_to_4byte_selector("getPaused(address)").hex()

# Row fields the Aave-fork supplementary reads may fill (the executor seeds
# these as None on every row of the plan and ignores any other key).
_AAVE_SUPPLEMENTARY_FIELDS = ("supply_cap", "borrow_cap", "emode_category", "is_paused")

# Supplementary call-id prefixes (the part before ":{symbol}") — dispatch keys
# for ``decode_supplementary``. Keep stable: goldens and log greps key on them.
_AAVE_CAPS_CALL_PREFIX = "aave_reserve_caps"
_AAVE_EMODE_CALL_PREFIX = "aave_reserve_emode"
_AAVE_PAUSED_CALL_PREFIX = "aave_reserve_paused"


def _strip_hex(raw_hex: str) -> str:
    if not isinstance(raw_hex, str):
        raise ValueError("payload is not a hex string")
    return raw_hex[2:] if raw_hex.startswith("0x") else raw_hex


def _decode_reserve_caps(raw_hex: str) -> dict[str, object]:
    """Decode ``getReserveCaps`` → exactly 2 words: (borrowCap, supplyCap).

    Strict length check: anything but exactly two 32-byte words (e.g. a
    10-word reserve-config blob from an old scripted mock, or an unrelated
    return shape on a divergent fork) raises ``ValueError`` → fail-open.
    """
    raw = _strip_hex(raw_hex)
    if len(raw) != 128:
        raise ValueError(f"getReserveCaps payload is not 2 words (len={len(raw)})")
    return {"borrow_cap": int(raw[0:64], 16), "supply_cap": int(raw[64:128], 16)}


def _decode_emode_category(raw_hex: str) -> dict[str, object]:
    """Decode ``getReserveEModeCategory`` → exactly 1 word (uint256)."""
    raw = _strip_hex(raw_hex)
    if len(raw) != 64:
        raise ValueError(f"getReserveEModeCategory payload is not 1 word (len={len(raw)})")
    return {"emode_category": int(raw, 16)}


def _decode_paused(raw_hex: str) -> dict[str, object]:
    """Decode ``getPaused`` → exactly 1 word holding a canonical bool (0/1).

    A non-canonical bool word is treated as a decode mismatch (fail-open to
    ``None``) rather than coerced — honest "unmeasured" beats a guessed flag.
    """
    raw = _strip_hex(raw_hex)
    if len(raw) != 64:
        raise ValueError(f"getPaused payload is not 1 word (len={len(raw)})")
    value = int(raw, 16)
    if value not in (0, 1):
        raise ValueError(f"getPaused payload is not a canonical bool (value={value})")
    return {"is_paused": value == 1}


def _aave_decode_supplementary(call_id: str, raw_hex: str) -> Mapping[str, object]:
    """Dispatch one supplementary payload by call-id prefix; ValueError on mismatch."""
    prefix = call_id.split(":", 1)[0]
    if prefix == _AAVE_CAPS_CALL_PREFIX:
        return _decode_reserve_caps(raw_hex)
    if prefix == _AAVE_EMODE_CALL_PREFIX:
        return _decode_emode_category(raw_hex)
    if prefix == _AAVE_PAUSED_CALL_PREFIX:
        return _decode_paused(raw_hex)
    raise ValueError(f"unknown supplementary call id: {call_id}")


def _is_measured_int(value: object) -> TypeGuard[int]:
    """True only for a measured integer — never ``None`` (unmeasured) or a bool.

    ``Empty != Zero``: an unmeasured field (``None``) must never satisfy a
    ``== 0`` / ``<= 10`` comparison, and a decoded flag that arrived as ``bool``
    must not masquerade as the int ``0``/``1`` (``False == 0`` is ``True``).
    """
    return isinstance(value, int) and not isinstance(value, bool)


def _aave_annotate_row(row: Mapping[str, object]) -> Mapping[str, str]:
    """Risk-context annotation for misleading rows.

    A reserve with ``usageAsCollateralEnabled == true`` can still contribute
    zero effective borrowing power in two measured shapes the flag columns
    hide, each surfaced here:

    * ``ltv == 0`` — collateral counts only inside an eMode category, or the
      asset is offboarded (eMode-only assets ezETH / rsETH / PT-*, offboarded
      DPI). With a fetched eMode category the note names it; a measured 0
      states there is none; an unmeasured category says so.
    * near-zero ``liquidationThreshold`` (``<= 10`` bps) while ``ltv`` itself is
      non-zero — a frozen / offboarding reserve that is flagged collateral but
      is effectively not liquidatable.

    Reports only what was measured and never guesses which case applies;
    unmeasured fields (``None``) never trigger a note (Empty != Zero).
    """
    if row.get("usage_as_collateral_enabled") is not True:
        return {}
    ltv = row.get("ltv_bps")
    if _is_measured_int(ltv) and ltv == 0:
        emode = row.get("emode_category")
        if _is_measured_int(emode) and emode > 0:
            return {"risk_note": f"base LTV zero — collateral counts only inside eMode category {emode}"}
        if _is_measured_int(emode) and emode == 0:
            return {"risk_note": "base LTV zero and eMode category 0 — no borrowing power (typical offboarding state)"}
        return {"risk_note": "base LTV zero — collateral only via eMode or offboarded (eMode category unmeasured)"}
    lt = row.get("liquidation_threshold_bps")
    if _is_measured_int(lt) and lt <= 10:
        return {
            "risk_note": (
                f"liquidation threshold near zero ({lt} bps) — flagged collateral but "
                "effectively not liquidatable (frozen / offboarding state)"
            )
        }
    return {}


def aave_fork_reserve_discovery_plan(protocol: str, provider_address: str) -> LendingReserveDiscoveryPlan:
    """Build the shared Aave-V3-fork plan (Aave, Spark, and future forks).

    Reuses the VIB-4925 selectors + decoders from ``aave_helpers`` — the same
    functions the VIB-3701/3749/3825 pre-flights consume — so decode behavior
    stays byte-identical to the v1 tool (behavior-parity contract).

    Adds three fail-open supplementary reads per reserve on the
    same PoolDataProvider (Spark inherits automatically): ``getReserveCaps``
    (supply/borrow caps in whole-token units; 0 = no cap in Aave semantics),
    ``getReserveEModeCategory``, and ``getPaused``. Forks whose deployed
    provider lacks one of these functions simply revert that inner call →
    those fields stay ``None`` (unmeasured).
    """
    from almanak.connectors._strategy_base.base.lending.aave_helpers import (
        _AAVE_GET_ALL_RESERVES_TOKENS_SELECTOR,
        _AAVE_GET_RESERVE_CONFIG_SELECTOR,
        decode_all_reserves_tokens,
        decode_reserve_configuration_data,
    )

    def _decode_enumeration(raw_hex: str) -> list[ReserveEntry]:
        tokens = decode_all_reserves_tokens(raw_hex)
        if tokens is None:
            raise ValueError("getAllReservesTokens() response failed to decode")
        entries: list[ReserveEntry] = []
        for symbol, address in tokens:
            asset_padded = address.lower().removeprefix("0x").zfill(64)
            entries.append(
                ReserveEntry(
                    symbol=symbol,
                    address=address.lower(),
                    config_call=ReserveCall(
                        to=provider_address,
                        data=_AAVE_GET_RESERVE_CONFIG_SELECTOR + asset_padded,
                        id=f"aave_reserve_cfg:{symbol}",
                    ),
                    supplementary_calls=(
                        ReserveCall(
                            to=provider_address,
                            data=_AAVE_GET_RESERVE_CAPS_SELECTOR + asset_padded,
                            id=f"{_AAVE_CAPS_CALL_PREFIX}:{symbol}",
                        ),
                        ReserveCall(
                            to=provider_address,
                            data=_AAVE_GET_RESERVE_EMODE_SELECTOR + asset_padded,
                            id=f"{_AAVE_EMODE_CALL_PREFIX}:{symbol}",
                        ),
                        ReserveCall(
                            to=provider_address,
                            data=_AAVE_GET_PAUSED_SELECTOR + asset_padded,
                            id=f"{_AAVE_PAUSED_CALL_PREFIX}:{symbol}",
                        ),
                    ),
                )
            )
        return entries

    def _decode_config(raw_hex: str) -> ReserveConfigRow:
        cfg = decode_reserve_configuration_data(raw_hex)
        if cfg is None:
            raise ValueError("getReserveConfigurationData() response failed to decode")
        return ReserveConfigRow(
            borrowing_enabled=cfg.borrowing_enabled,
            usage_as_collateral_enabled=cfg.usage_as_collateral_enabled,
            is_active=cfg.is_active,
            is_frozen=cfg.is_frozen,
            ltv_bps=cfg.ltv,
            liquidation_threshold_bps=cfg.liquidation_threshold,
        )

    return LendingReserveDiscoveryPlan(
        protocol=protocol,
        provider_address=provider_address,
        enumeration_call=ReserveCall(
            to=provider_address,
            data=_AAVE_GET_ALL_RESERVES_TOKENS_SELECTOR,
            id="aave_all_reserves",
        ),
        decode_enumeration=_decode_enumeration,
        decode_config=_decode_config,
        supplementary_fields=_AAVE_SUPPLEMENTARY_FIELDS,
        decode_supplementary=_aave_decode_supplementary,
        annotate_row=_aave_annotate_row,
    )
