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
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

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
    decoded by the plan's ``decode_config``) or ``static_config``."""

    symbol: str
    address: str
    config_call: ReserveCall | None = None
    static_config: ReserveConfigRow | None = None


@dataclass(frozen=True)
class LendingReserveDiscoveryPlan:
    """Pure reserve-discovery plan for one ``(protocol, chain)``.

    Exactly one of (``enumeration_call`` + ``decode_enumeration``) or
    ``static_entries`` must be set. ``decode_config`` is required when any
    entry carries a ``config_call``.
    """

    protocol: str
    provider_address: str
    enumeration_call: ReserveCall | None = None
    decode_enumeration: Callable[[str], list[ReserveEntry]] | None = None
    static_entries: tuple[ReserveEntry, ...] | None = None
    decode_config: Callable[[str], ReserveConfigRow] | None = None

    def __post_init__(self) -> None:
        has_call = self.enumeration_call is not None
        has_decode = self.decode_enumeration is not None
        has_static = self.static_entries is not None
        if has_call != has_decode:
            raise ValueError(f"{self.protocol}: enumeration_call and decode_enumeration must be set together")
        if has_call == has_static:
            raise ValueError(f"{self.protocol}: plan must be exactly one of call-enumerated or static-enumerated")


def aave_fork_reserve_discovery_plan(protocol: str, provider_address: str) -> LendingReserveDiscoveryPlan:
    """Build the shared Aave-V3-fork plan (Aave, Spark, and future forks).

    Reuses the VIB-4925 selectors + decoders from ``aave_helpers`` — the same
    functions the VIB-3701/3749/3825 pre-flights consume — so decode behavior
    stays byte-identical to the v1 tool (behavior-parity contract).
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
    )
