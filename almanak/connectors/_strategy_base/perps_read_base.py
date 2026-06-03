"""Strategy-side shared infrastructure for connector perp-position reads.

Mirrors the lending account-state seam (:mod:`lending_read_base`). The framework
perp reader/valuer must not hardcode which on-chain contracts hold a venue's
position data, how to decode them, or how to value a position — that is
**connector knowledge**. This module owns the venue-neutral types every perp
connector that exposes a "read + value open positions" capability shares; each
connector publishes a concrete :class:`PerpsReadSpec` (pure data + pure
functions) that the strategy-side :class:`~almanak.connectors._strategy_base.perps_read_registry.PerpsReadRegistry`
dispatches to.

Two responsibilities, both pure:

* :class:`PerpsReadSpec` describes a *read* (which contract-role addresses to
  resolve, the calldata planner, the return decoder) and *values* an
  already-decoded position (the venue's mark-to-market formula).
* :class:`PerpsReadResult` carries the Empty≠Zero distinction at the read
  boundary: ``ok=True`` with no positions is a *measured* empty; ``ok=False`` is
  an *unmeasured* failed read (the framework keeps the strategy-reported value,
  never fabricates a zero).

Gateway-boundary note (same as :mod:`lending_read_base`): this module performs
**no** network egress. It only *describes* reads and *values* decoded positions;
the gateway-routed ``eth_call`` that executes a read stays in the framework
reader, which owns the gateway client.

VIB-4930 (epic VIB-4851).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal

# ``EthCall`` and the ABI helpers are venue-neutral primitives that already live
# on the lending seam; import (and re-export) them rather than defining a second
# copy. Their home in ``lending_read_base`` is incidental — a future cleanup may
# relocate them to a neutral module, at which point only this import changes.
from almanak.connectors._strategy_base.lending_read_base import (
    EthCall,
    decode_uint_hex,
    pad_address,
)

__all__ = [
    "EthCall",
    "PerpsMarketMeta",
    "PerpsPositionOnChain",
    "PerpsPositionPlan",
    "PerpsPositionQuery",
    "PerpsPositionValue",
    "PerpsReadResult",
    "PerpsReadSpec",
    "decode_uint_hex",
    "pad_address",
]


@dataclass(frozen=True)
class PerpsPositionOnChain:
    """Decoded on-chain state of a single perpetual position (raw, venue-neutral).

    Relocated from the framework perp reader. Values are raw (not human-readable)
    to preserve precision; the connector's ``value_position`` converts. A
    ``PerpsPositionOnChain`` only exists for a *successfully decoded* position, so
    every field is a measured value — the Empty≠Zero "unmeasured" state lives at
    the list level (:class:`PerpsReadResult`), never as a fabricated field value.

    ``key_prefix`` is per-connector and **cosmetic**: no consumer matches on
    ``position_key`` (valuation matches by market + direction + collateral; the
    accounting layer uses a separate ``perp:{protocol}:…`` namespace). GMX emits
    ``"gmx"`` so the key stays byte-identical to the pre-VIB-4930 reader; a second
    venue sets its own prefix without a framework edit.
    """

    account: str
    market: str  # Market contract address (the valuation join key)
    collateral_token: str  # Collateral token address
    size_in_usd: int  # GMX: 30 decimals
    size_in_tokens: int  # Index token decimals
    collateral_amount: int  # Collateral token decimals
    is_long: bool
    borrowing_factor: int
    funding_fee_amount_per_size: int
    increased_at_time: int  # Unix timestamp
    decreased_at_time: int  # Unix timestamp
    key_prefix: str = "gmx"

    @property
    def is_active(self) -> bool:
        """Position has non-zero size."""
        return self.size_in_usd > 0

    @property
    def position_key(self) -> str:
        """Stable id matching strategy-reported position ids (cosmetic prefix)."""
        side = "long" if self.is_long else "short"
        return f"{self.key_prefix}-{self.market.lower()}-{self.collateral_token.lower()}-{side}"


@dataclass(frozen=True)
class PerpsReadResult:
    """Outcome of a perp position read: the positions plus whether the read ran.

    The Empty≠Zero seam (AGENTS.md §Accounting). ``ok=True`` with an empty
    ``positions`` tuple is a *measured* "wallet has no positions"; ``ok=False`` is
    an *unmeasured* failed / reverted read — the framework keeps the
    strategy-reported value rather than fabricating a zero. A connector reducer
    returns ``ok=False`` only when the read itself failed, never to signal an
    empty book.
    """

    positions: tuple[PerpsPositionOnChain, ...]
    ok: bool


@dataclass(frozen=True)
class PerpsPositionValue:
    """Valued perpetual position for a single market.

    All USD values are human-readable (not raw venue-decimal). Relocated, shape
    unchanged, from the framework perp valuer so it is the shared result type the
    connector ``value_position`` returns and the framework consumes.
    """

    market: str
    is_long: bool
    size_usd: Decimal  # Notional size in USD
    collateral_value_usd: Decimal  # Collateral marked to market
    entry_price_usd: Decimal  # Average entry price
    mark_price_usd: Decimal  # Current market price
    unrealized_pnl_usd: Decimal  # Position PnL before fees
    pending_fees_usd: Decimal  # Funding + borrowing fees owed
    net_value_usd: Decimal  # collateral + pnl - fees (what you'd get closing)
    leverage: Decimal  # size / collateral


@dataclass(frozen=True)
class PerpsMarketMeta:
    """Connector-resolved market metadata needed to value a position.

    Returned by a spec's ``market_metadata(market_address, chain)``; ``None`` when
    the market is unknown (callers fail closed rather than guessing decimals).
    """

    index_token_symbol: str
    index_token_decimals: int


@dataclass(frozen=True)
class PerpsPositionQuery:
    """A resolved perp-position read request.

    Built by the strategy-side registry (which resolves each declared
    contract-role address) and consumed by a connector's pure ``build_calls``.

    Attributes:
        chain: Chain identifier (e.g. ``"arbitrum"``).
        wallet_address: Wallet whose open positions are read.
        targets: Generic ``role -> resolved address`` map. Kept generic so the
            registry never names a venue-specific contract role — GMX reads
            ``targets["reader"]`` / ``targets["data_store"]``, Aster reads
            ``targets["router"]``. ``None`` before the registry resolves it.
        markets: Per-market venues (e.g. Aster) read one market per call, so the
            registry fills this from the spec's ``markets_for_chain``; empty for
            range-read venues (GMX returns the whole book in one call).
        block: Block to pin the read to; ``None`` → ``"latest"``.
    """

    chain: str
    wallet_address: str
    targets: Mapping[str, str] | None = None
    markets: tuple[str, ...] = ()
    block: int | str | None = None


@dataclass(frozen=True)
class PerpsReadSpec:
    """Connector-published descriptor for a perp position read + valuation.

    The perp analogue of
    :class:`~almanak.connectors._strategy_base.lending_read_base.AccountStateReadSpec`.
    Pure: it *describes* reads and *values* decoded positions; it never touches
    the gateway.

    Attributes:
        contract_kinds: ``role -> ordered AddressRegistry contract-kind names``
            the registry resolves the read target(s) from. GMX:
            ``{"reader": ("reader",), "data_store": ("data_store",)}``; Aster:
            ``{"router": ("router",)}``. The role keys are the connector's own
            vocabulary, indexed by the connector's ``build_calls`` — the registry
            treats them opaquely.
        build_calls: ``PerpsPositionQuery -> list[EthCall]`` planner (pure).
        reduce_calls: ``(query, results) -> PerpsReadResult`` reducer (pure;
            ``ok=False`` on a failed read, never a fabricated empty).
        market_metadata: ``(market_address, chain) -> PerpsMarketMeta | None``
            (``None`` on an unknown market).
        value_position: the venue's pure mark-to-market formula (keyword-only),
            returning :class:`PerpsPositionValue`.
        position_key_prefix: the per-venue ``PerpsPositionOnChain.key_prefix`` the
            reducer stamps (``"gmx"`` keeps GMX keys byte-identical).
        markets_for_chain: ``chain -> markets`` for per-market venues; ``None``
            for range-read venues (GMX).
    """

    contract_kinds: Mapping[str, tuple[str, ...]]
    build_calls: Callable[[PerpsPositionQuery], list[EthCall]]
    reduce_calls: Callable[[PerpsPositionQuery, list[str | None]], PerpsReadResult]
    market_metadata: Callable[[str, str], PerpsMarketMeta | None]
    value_position: Callable[..., PerpsPositionValue]
    position_key_prefix: str = "gmx"
    markets_for_chain: Callable[[str], tuple[str, ...]] | None = None


@dataclass(frozen=True)
class PerpsPositionPlan:
    """A fully materialised perp read for one ``(protocol, chain)``.

    Produced by
    :meth:`~almanak.connectors._strategy_base.perps_read_registry.PerpsReadRegistry.resolve_plan`.
    The framework reader needs only the gateway client to execute it: the ordered
    :class:`EthCall` reads plus the connector's pure reducer. Valuation and market
    metadata are reached separately, via the registry by protocol, so the plan
    stays minimal — mirroring the lending ``AccountStatePlan``.
    """

    query: PerpsPositionQuery
    calls: tuple[EthCall, ...]
    reduce: Callable[[PerpsPositionQuery, list[str | None]], PerpsReadResult]
