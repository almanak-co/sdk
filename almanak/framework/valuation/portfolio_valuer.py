"""Portfolio valuation orchestrator.

Produces PortfolioSnapshot by querying the gateway (via MarketSnapshot)
for wallet balances and token prices, using the PositionDiscoveryService
to proactively find on-chain positions (LP, lending, perps), with
strategy.get_open_positions() as a supplementary source.

This is the single source of truth for portfolio valuation at runtime.
The framework owns both discovery and math.

The projection contract (what total_value_usd / deployed_capital_usd / debt_mark /
NAV each mean, and the lending signed-leg convention) is documented and
invariant-tested per blueprint 27 §7.11 "PortfolioValuer projection contract"
(VIB-5206) — see tests/unit/valuation/test_portfoliovaluer_contract.py. The
``debt_mark`` / net-equity-cost terms are computed in the read path (not stamped on
the snapshot) by the canonical ``valuation/net_debt.py::compute_net_debt_projection``
(VIB-5222) — the single netting implementation every consumer routes through.
"""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry
from almanak.framework.portfolio.models import (
    PortfolioSnapshot,
    PositionValue,
    TokenBalance,
    ValueConfidence,
)
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.fungible_lp_position_reader import FungibleLpPositionReader
from almanak.framework.valuation.lending_position_reader import LendingPositionReader
from almanak.framework.valuation.lending_valuer import value_lending_position
from almanak.framework.valuation.lp_position_reader import LPPositionReader
from almanak.framework.valuation.lp_valuer import value_lp_position
from almanak.framework.valuation.perps_position_reader import PerpsPositionReader
from almanak.framework.valuation.position_discovery import (
    DiscoveryConfig,
    PositionDiscoveryService,
)
from almanak.framework.valuation.spot_valuer import total_value, value_tokens
from almanak.framework.valuation.vault_position_reader import VaultPositionReader

if TYPE_CHECKING:
    from almanak.connectors._strategy_base.perps_read_base import PerpsPositionValue
    from almanak.framework.teardown.models import TeardownPositionSummary
    from almanak.framework.valuation.lp_position_reader import LPPositionOnChain

logger = logging.getLogger(__name__)

FRAMEWORK_EXTERNAL_AGREEMENT_THRESHOLD = Decimal("0.10")
FRAMEWORK_EXTERNAL_DIVERGENCE_THRESHOLD = Decimal("0.20")

# VIB-5018 / VIB-4586 — Uniswap V4 LP positions do NOT live on the V3
# NonfungiblePositionManager. Routing a V4 tokenId through the V3-shaped
# ``LPPositionReader`` reads an unrelated NFT (or garbage) on the V3 PM and
# corrupts BOTH token identity (token0_symbol="link" on a WETH/USDC pool) and
# amount scaling (~10^7), producing a $289M value for a ~$5 position at HIGH
# confidence. The V4 stream has its own isolated valuation path
# (``_reprice_v4_lp_enriched``) keyed off the canonical PoolKey resolved via the
# gateway — never a V3 NFT read. V4 is detected by DATA SHAPE (a 64-hex PoolKey
# hash in ``details``), not by a hardcoded protocol name — the VIB-4636
# capability-gate discipline that keeps the framework free of connector-name
# coupling. A V4 pool has no contract address (singleton PoolManager), so a
# 64-hex value in the pool slot is unambiguously a V4 pool_id; a V3 LP carries a
# 40-hex pool *contract* address (or none).
_V4_POOL_ID_HEX_LEN = 64

# VIB-5006: Aave-fork interest rates (``liquidityRate``) are reported in ray
# (1e27 fixed-point). Divide by this and ×100 to render a human percentage.
_RAY_SCALE = Decimal("1e27")


def _looks_like_evm_address(value: object) -> bool:
    """Return True iff ``value`` is the 42-char ``0x``-prefixed hex shape.

    VIB-4274 — ``position.details["pool"]`` is type-overloaded across the
    codebase: some producers stash an actual pool contract address
    (``"0x..."``), others stash a human descriptor (``"WETH/USDC/500"``).
    The descriptor shape silently slipped into ``eth_call`` for slot0 reads
    and triggered ``-32602 odd number of digits`` warnings every snapshot
    (hidden by the price-ratio-tick fallback so the 21-cell Accountant Test
    still passed, but the ``in_range`` flag would silently lie on mainnet).

    Mirrors the VIB-3943 guard at
    ``almanak/framework/teardown/post_conditions.py:209`` so the two consumer
    sites stay aligned. Any drift here should be paired with a drift there.
    """
    return (
        isinstance(value, str)
        and value.startswith("0x")
        and len(value) == 42
        and all(c in "0123456789abcdefABCDEF" for c in value[2:])
    )


def _resolve_lp_pool_address_from_details(position: Any) -> str | None:
    """Resolve a usable pool address from ``position.details`` for an LP repricing path.

    VIB-4274 — `position.details["pool"]` is type-overloaded (descriptor vs hex
    address); `pool_address` is the canonical key for the actual contract.
    Read order matches the post-VIB-3943 defensive convention in
    ``teardown/post_conditions.py:189`` — ``pool_address`` first, ``pool``
    as a legacy fallback. Non-hex values (descriptor strings like
    ``"WETH/USDC/500"``) are rejected and the caller should fall through
    to the price-ratio-tick approximation.

    Extracted to a single helper so the two LP repricing paths
    (``_reprice_lp_on_chain_enriched`` and ``_reprice_lp_on_chain``) stay
    aligned automatically — gemini-code-assist flagged the duplicated
    guard at PR #2231 review.

    Returns the validated 42-char ``0x``-prefixed hex address, or ``None``
    when no usable address is available.
    """
    pool_address = position.details.get("pool_address") or position.details.get("pool")
    if pool_address and not _looks_like_evm_address(pool_address):
        return None
    return pool_address or None


@dataclass(frozen=True)
class _WalletMatchIndex:
    """Pre-built lookup sets for O(1) wallet-overlap detection (VIB-4909).

    Building once per snapshot — see ``_build_wallet_match_index`` — keeps
    the per-position classifier O(1) instead of O(M wallet entries), which
    matters when strategies enumerate hundreds of positions (NFT-LP sweeps
    etc.). Today's wallets are ~10 entries, so this is forward-looking
    rather than a hot-path optimization.
    """

    symbols: frozenset[str]  # case-folded
    evm_addresses: frozenset[str]  # case-folded, EVM-shape only (0x + 40 hex)
    exact_addresses: frozenset[str]  # non-EVM (e.g. Solana base58, case-significant)


def _is_evm_address_shape(value: str) -> bool:
    """0x-prefixed 42-char ASCII hex. Case-folding such an address is
    semantically safe (EVM addresses are case-insensitive at the protocol
    layer; EIP-55 checksum is a *display* convention).

    Defined here in addition to ``_looks_like_evm_address`` because the two
    callers have slightly different needs: the LP repricer wants a strict
    *rejection* signal for non-EVM strings, while the wallet-overlap matcher
    wants to *choose between* case-folded and exact comparison. Keeping the
    intent locally readable beats a single all-purpose helper.
    """
    return len(value) == 42 and value.startswith("0x") and all(c in "0123456789abcdefABCDEF" for c in value[2:])


def _build_wallet_match_index(wallet_balances: list[Any]) -> _WalletMatchIndex:
    """Build the per-snapshot wallet-overlap index in a single pass.

    Returns a frozen index of:
    - case-folded ``symbol`` strings,
    - case-folded ``address`` strings for EVM-shape addresses,
    - exact-case ``address`` strings for non-EVM addresses (Solana base58
      is case-significant — case-folding "AB" and "ab" would conflate
      semantically distinct on-chain accounts).
    """
    symbols: set[str] = set()
    evm_addresses: set[str] = set()
    exact_addresses: set[str] = set()
    for tb in wallet_balances:
        sym = getattr(tb, "symbol", None)
        if isinstance(sym, str) and sym:
            symbols.add(sym.casefold())
        addr = getattr(tb, "address", None)
        if isinstance(addr, str) and addr:
            if _is_evm_address_shape(addr):
                evm_addresses.add(addr.casefold())
            else:
                exact_addresses.add(addr)
    return _WalletMatchIndex(
        symbols=frozenset(symbols),
        evm_addresses=frozenset(evm_addresses),
        exact_addresses=frozenset(exact_addresses),
    )


def _is_wallet_pseudo_position(
    position: Any,
    wallet_balances: list[Any],
) -> bool:
    """Return True iff ``position`` is a TOKEN-class wallet pseudo-position.

    VIB-4909 — ``PositionType.TOKEN`` is sometimes a wallet pseudo-position
    (SWAP-class strategies report a wallet token as a TOKEN "position" for
    teardown enumeration and operator visibility — the value already lives
    in ``wallet_balances``) and sometimes a deployed holding that is
    NOT in ``wallet_balances`` (e.g. ``metamorpho_eth_yield`` surfaces vault
    shares as a TOKEN position while the wallet tracks the deposit token).

    To prevent both double-counting (the SWAP-class case) and under-counting
    (the vault-shares case) in ``wallet_total_value_usd``, classify per
    position by checking whether the position's underlying asset overlaps
    with the wallet:

    - ``position.details["asset"]`` matched case-insensitively against
      ``TokenBalance.symbol`` of every wallet entry, OR
    - ``position.details["address"]`` matched against ``TokenBalance.address``.
      Case-folded for EVM-shape addresses; exact-match otherwise (Solana
      base58 is case-significant).

    Strategies whose TOKEN-position details carry neither key (e.g.
    ``metamorpho_eth_yield`` with only ``vault_address`` / ``deposit_token``,
    or ``pendle_basics`` with ``pt_token`` / ``base_token``) are treated as
    NON-overlapping and contribute to ``wallet_total_value_usd`` — the
    defensive choice, since dropping a deployed holding silently is a
    much worse failure than the legacy double-count this fix targets.

    Returns False for any non-TOKEN PositionType — those are real protocol
    positions (LP / SUPPLY / BORROW / PERP / VAULT / STAKE / PREDICTION /
    CEX) that are never represented in ``wallet_balances`` and always
    contribute to the formula.
    """
    if position.position_type != PositionType.TOKEN:
        return False
    return _token_overlaps_wallet_index(position, _build_wallet_match_index(wallet_balances))


def _token_overlaps_wallet_index(
    position: Any,
    index: _WalletMatchIndex,
) -> bool:
    """Fast classifier — assumes caller has filtered to TOKEN positions.

    Used by the writer's comprehension where the wallet index is built
    once per snapshot and reused across every position. Direct callers
    (tests, dashboards) should prefer ``_is_wallet_pseudo_position`` which
    builds the index internally.
    """
    details = getattr(position, "details", None) or {}

    asset_symbol = details.get("asset")
    if isinstance(asset_symbol, str) and asset_symbol and asset_symbol.casefold() in index.symbols:
        return True

    asset_addr = details.get("address")
    if isinstance(asset_addr, str) and asset_addr:
        if _is_evm_address_shape(asset_addr):
            if asset_addr.casefold() in index.evm_addresses:
                return True
        elif asset_addr in index.exact_addresses:
            return True

    return False


# ---------------------------------------------------------------------------
# Swap-inventory classification (VIB-5057)
# ---------------------------------------------------------------------------
#
# A spot TA / swap strategy's deployed capital IS wallet-held tokens, bought
# with strategy capital and tracked as open FIFO swap-inventory lots
# (``FIFOBasisStore.iter_open_swap_lots``, ``source == "SWAP"``). Pre-VIB-5057
# the snapshot writer classified the ENTIRE wallet token value as
# ``available_cash_usd``, so the dashboard money trail permanently read
# "Available wallet cash ≈ 100% of wallet NAV / Open position NAV $0.00 /
# Open cost basis $0.00" even mid-position. The classifier below moves the
# open-lot inventory to the deployed side of the split:
#
#   * ``available_cash_usd`` = wallet value − open-swap-lot inventory value,
#   * inventory surfaces as visible ``PositionType.TOKEN`` rows that count
#     into ``total_value_usd`` (open-position NAV),
#   * ``deployed_capital_usd`` gains the FIFO cost basis of the open lots.
#
# Wallet NAV (``total_value_usd + available_cash_usd``) and
# ``wallet_total_value_usd`` are INVARIANT under the reclassification — only
# the split moves (blueprint 27 §3.4 S9 no-double-count). The swap-MTM
# attribution fix (PR #2648) recovered the *PnL* half via a gateway-side
# additive term; that term is suppressed for snapshots stamped
# ``swap_inventory.status == "applied"`` (see
# ``dashboard_service.GetCostStack``) so inventory MTM enters Strategy PnL
# exactly once in every writer/reader version combination.

# ``details["source"]`` marker on the synthetic inventory rows. Data-shape
# gate (VIB-4636 discipline): consumers detect inventory rows by this marker,
# never by protocol-name string coupling.
_SWAP_INVENTORY_SOURCE = "swap_inventory_lots"


def _is_swap_inventory_row(position: Any) -> bool:
    """True iff ``position`` is a lot-derived deployed-inventory row (VIB-5057).

    These rows are wallet-held (their value lives in ``wallet_balances`` /
    ``available_cash``'s pre-split wallet value) but represent DEPLOYED
    strategy capital, so the snapshot sums treat them inversely to the
    VIB-4909 wallet pseudo-positions: included in ``total_value_usd``,
    excluded from the ``wallet_total_value_usd`` position add (already
    counted once in wallet value).
    """
    if position.position_type != PositionType.TOKEN:
        return False
    details = getattr(position, "details", None) or {}
    return details.get("source") == _SWAP_INVENTORY_SOURCE


@dataclass(frozen=True)
class _SwapInventoryClassification:
    """Result of classifying open swap-inventory lots for one snapshot.

    ``metadata`` is stamped onto ``snapshot_metadata["swap_inventory"]`` when
    not ``None``; it is ``None`` exactly when there is nothing to report (no
    accounting context, or a healthy event stream with zero open swap lots) so
    non-swap strategies stay byte-identical to the pre-VIB-5057 writer.
    """

    rows: list[PositionValue]
    inventory_value_usd: Decimal
    metadata: dict[str, Any] | None


_NO_SWAP_INVENTORY = _SwapInventoryClassification([], Decimal("0"), None)


def _aggregate_open_swap_lots(
    events: list[dict[str, Any]],
    deployment_id: str,
) -> dict[str, tuple[Decimal, Decimal | None]]:
    """Aggregate open swap-inventory lots per token from accounting events.

    Replays the deployment-scoped event history through ``FIFOBasisStore``
    (the same reconstruction the dashboard's ``compute_inventory_unrealized``
    uses — reconstruction from durable events sidesteps the runner's in-memory
    lot store entirely, so a restart cannot silently zero the inventory) and
    sums ``iter_open_swap_lots`` per case-folded token symbol.

    Returns ``{token_key: (remaining_total, cost_total)}`` where ``cost_total``
    is ``None`` when ANY of the token's open lots has an unmeasured cost basis
    (Empty ≠ Zero — one unmeasured lot poisons the token's whole basis; a
    partial sum would understate cost and overstate unrealized PnL).

    Events are scoped to ``deployment_id`` before replay so a shared wallet
    cannot leak a co-located strategy's inventory into this snapshot (same
    fail-closed rule as ``compute_inventory_unrealized``).
    """
    from almanak.framework.accounting.basis import FIFOBasisStore

    scoped = [ev for ev in events if isinstance(ev, dict) and ev.get("deployment_id") == deployment_id]
    store = FIFOBasisStore()
    store.reconstruct_from_events(scoped)

    totals: dict[str, tuple[Decimal, Decimal | None]] = {}
    for _position_key, token, remaining, cost_for_remaining in store.iter_open_swap_lots():
        key = token.casefold()
        prev_remaining, prev_cost = totals.get(key, (Decimal("0"), Decimal("0")))
        cost = None if (prev_cost is None or cost_for_remaining is None) else prev_cost + cost_for_remaining
        totals[key] = (prev_remaining + remaining, cost)
    return totals


def _classify_swap_inventory(
    lot_totals: dict[str, tuple[Decimal, Decimal | None]],
    balances: dict[str, Decimal],
    prices: dict[str, Decimal],
    chain: str,
) -> _SwapInventoryClassification:
    """Classify per-token open-lot inventory as deployed capital (VIB-5057).

    Per token, the classification is whole-or-nothing with explicit skip
    reasons (never a silent $0 booking):

    * ``cost_unmeasured`` — the token's FIFO basis is ``None`` (e.g. degraded
      SWAP events missing ``amount_out_usd``). Moving the value while booking
      a fabricated ``Decimal("0")`` basis would overstate unrealized PnL by
      the full mark (Empty ≠ Zero), so the token stays classified as cash —
      exactly the pre-fix behaviour.
    * ``capped_to_zero`` — the wallet does not currently hold the token
      (stale lots / full external transfer). Nothing to move; cash unchanged.
    * ``price_missing`` — the wallet holds the token but no mark price is
      available. The token is absent from the wallet value too
      (``value_tokens`` drops unpriced tokens), so skipping keeps the split
      symmetric; the pre-existing ``wallet_data_incomplete`` path already
      downgrades snapshot confidence.

    When the wallet holds LESS than the open-lot quantity (partial external
    transfer, or stale lots), the inventory is capped at the wallet's actual
    holding and the cost basis is pro-rated by the same ratio — so
    ``available_cash_usd`` can never go negative by construction (per token,
    inventory value ≤ wallet token value).
    """
    if not lot_totals:
        return _NO_SWAP_INVENTORY

    # Case-variant duplicate symbols (e.g. "USDC" and "usdc" both tracked) must
    # SUM, not last-write-wins: wallet_value already counts both balances, so a
    # partial cap would understate inventory and overstate available_cash — the
    # exact symptom this classifier fixes. First-seen symbol wins for display.
    balance_by_key: dict[str, tuple[str, Decimal]] = {}
    for sym, bal in balances.items():
        key = sym.casefold()
        display, total = balance_by_key.get(key, (sym, Decimal("0")))
        balance_by_key[key] = (display, total + bal)
    price_by_key = {sym.casefold(): px for sym, px in prices.items()}

    rows: list[PositionValue] = []
    skipped: dict[str, str] = {}
    token_detail: dict[str, dict[str, Any]] = {}
    total_value = Decimal("0")
    total_cost = Decimal("0")

    for key in sorted(lot_totals):
        remaining, cost = lot_totals[key]
        display, wallet_qty = balance_by_key.get(key, (key.upper(), None))
        price = price_by_key.get(key)
        if cost is None:
            skipped[key] = "cost_unmeasured"
            continue
        if wallet_qty is None or wallet_qty <= 0:
            skipped[key] = "capped_to_zero"
            continue
        if price is None or price <= 0:
            skipped[key] = "price_missing"
            continue

        quantity = min(remaining, wallet_qty)
        capped = quantity < remaining
        cost_for_quantity = cost * (quantity / remaining) if capped else cost
        value = quantity * price
        total_value += value
        total_cost += cost_for_quantity
        token_detail[key] = {
            "quantity": str(quantity),
            "value_usd": str(value),
            "cost_usd": str(cost_for_quantity),
            "capped": capped,
        }
        rows.append(
            PositionValue(
                position_type=PositionType.TOKEN,
                protocol="wallet",
                chain=chain,
                value_usd=value,
                label=f"swap inventory {display}",
                tokens=[display],
                details={
                    "asset": display,
                    "source": _SWAP_INVENTORY_SOURCE,
                    "classification": "deployed_inventory",
                    "quantity": str(quantity),
                    "lot_remaining": str(remaining),
                    "capped": capped,
                },
                cost_basis_usd=cost_for_quantity,
                unrealized_pnl_usd=value - cost_for_quantity,
            )
        )

    metadata: dict[str, Any] = {"status": "applied" if rows else "unmeasured"}
    if rows:
        metadata["value_usd"] = str(total_value)
        metadata["cost_usd"] = str(total_cost)
        metadata["tokens"] = token_detail
    if skipped:
        metadata["skipped"] = skipped
    return _SwapInventoryClassification(rows, total_value, metadata)


# ---------------------------------------------------------------------------
# Held-PT inventory classification (VIB-5316)
# ---------------------------------------------------------------------------
#
# A held Pendle PT is NOT a position_event: the ``PENDLE_PT`` PositionType was
# removed in VIB-4931 and ``SWAP`` is absent from ``INTENT_TO_EVENT_TYPE``, so a
# strategy that swapped into a PT reports NO discovered position for it — the
# VIB-5313 reprice path (``_reprice_principal_token_enriched``) is INERT for
# such a strategy. This classifier is the LIVE consumer that makes the gateway
# PT/USD valuation reach NAV: it synthesizes the open-PT inventory from FIFO
# basis lots (``FIFOBasisStore.iter_open_pt_lots`` — ``PT_BUY`` minus matched
# ``PT_SELL`` / ``PT_REDEEM``) and values each open symbol via the gateway price
# authority (``MarketSnapshot.pt_price`` → ``value_principal_token_position``,
# design spine §2 VIB-5316). NO ``position_events`` / ``PositionType`` change
# (default-(b)) — the FIFO lot IS the inventory record.
#
# Unlike swap inventory (VIB-5057), a PT is KNOWN_UNPRICEABLE in the spot oracle
# and is dropped from the wallet token valuation — so its USD value is NOT
# already counted in ``wallet_value``. The synthetic PT rows are therefore
# ordinary NON-wallet positions: they count into ``total_value_usd``
# (open-position NAV) AND ``wallet_total_value_usd`` (counted once — they do not
# overlap the wallet index since the PT symbol is unpriceable / absent from
# wallet balances), and their cost basis flows into ``deployed_capital_usd``.
# They are NOT subtracted from ``available_cash_usd`` (a held PT is not cash).
#
# ``details["source"]`` is the data-shape marker (VIB-4636 discipline — consumers
# detect these rows by the marker, never by a protocol-name string; this file
# stays free of connector-name coupling so the framework→connector ratchet,
# ``scripts/ci/scan_chain_protocol_coupling.py``, stays green).
_PT_INVENTORY_SOURCE = "pt_inventory_lots"


@dataclass(frozen=True)
class _PtInventoryClassification:
    """Result of classifying open held-PT inventory for one snapshot (VIB-5316).

    ``metadata`` is stamped onto ``snapshot_metadata["pt_inventory"]`` when not
    ``None``; it is ``None`` exactly when there is nothing to report (no
    accounting context, or zero open PT lots) so non-Pendle strategies stay
    byte-identical to the pre-VIB-5316 writer.
    """

    rows: list[PositionValue]
    metadata: dict[str, Any] | None


_NO_PT_INVENTORY = _PtInventoryClassification([], None)


def _reported_pt_symbols(positions: list[PositionValue]) -> set[str]:
    """Canonical PT symbols already represented by a discovered position.

    The VIB-5313 reprice path values a PT a strategy reports as a discovered
    position (``details.pt_token`` / ``pt_symbol``). FIFO inventory must only
    FILL THE GAP for held PTs nothing else surfaces — never double-count a
    symbol the reprice path already valued.

    VIB-5355: keyed by ``canonical_pt_symbol`` (maturity-INSENSITIVE for PTs) so
    a discovered position's maturity-LESS config symbol (``PT-wstETH``, from
    ``details.pt_token``) matches the FIFO inventory's maturity-BEARING ledger
    symbol (``PT-wstETH-25JUN2026``). Bare ``canonical_symbol`` left the skip-set
    as ``{PT-WSTETH}`` while FIFO aggregated under ``PT-WSTETH-25JUN2026``, so the
    skip MISSED and the same held PT was counted by BOTH the reprice path and the
    FIFO inventory path → ~2× NAV. The skip-comparison side in
    :func:`_classify_pt_inventory` folds the FIFO display symbol through the same
    helper so both ends of the dedup agree.
    """
    from almanak.framework.accounting.basis import canonical_pt_symbol

    symbols: set[str] = set()
    for p in positions:
        details = getattr(p, "details", None) or {}
        raw = details.get("pt_token") or details.get("pt_token_symbol") or details.get("pt_symbol")
        if isinstance(raw, str) and raw:
            symbols.add(canonical_pt_symbol(raw))
    return symbols


def _aggregate_open_pt_lots(
    events: list[dict[str, Any]],
    deployment_id: str,
) -> dict[str, tuple[Decimal, Decimal | None, Decimal | None, str]]:
    """Aggregate open held-PT lots per symbol from accounting events (VIB-5316).

    Replays the deployment-scoped event history through ``FIFOBasisStore`` (the
    same reconstruction the swap-inventory path uses — reconstruction from
    durable events sidesteps the runner's in-memory lot store so a restart cannot
    silently zero the inventory) and sums ``iter_open_pt_lots`` per
    ``canonical_symbol`` (the case-insensitive identity + join + FIFO-match key,
    spine §3.1).

    Returns ``{canonical_symbol: (remaining_pt_total, sy_cost_total,
    usd_cost_total, display)}`` where:

    * ``sy_cost_total`` — the SY/underlying-denominated cost of the open inventory.
    * ``usd_cost_total`` — the BUY-TIME-anchored USD cost basis of the open
      inventory (``cost_per_pt × remaining × underlying_price_at_buy`` summed
      across lots). This is the COST the valuer uses; it is NOT re-marked at the
      current underlying price (the VIB-5316 bug).

    Both totals are ``None`` when ANY of the symbol's open lots has an unmeasured
    contribution (Empty ≠ Zero — one unmeasured lot poisons the symbol's whole
    basis; a pre-fix lot with no buy-time price poisons ``usd_cost_total`` only).
    ``display`` is the first-seen ORIGINAL-case symbol (canonical keys join /
    dedup; the original case is kept for the dashboard, mirroring swap inventory).

    Events are scoped to ``deployment_id`` before replay so a shared wallet cannot
    leak a co-located strategy's PT inventory into this snapshot.
    """
    from almanak.framework.accounting.basis import FIFOBasisStore, canonical_symbol

    scoped = [ev for ev in events if isinstance(ev, dict) and ev.get("deployment_id") == deployment_id]
    store = FIFOBasisStore()
    store.reconstruct_from_events(scoped)

    totals: dict[str, tuple[Decimal, Decimal | None, Decimal | None, str]] = {}
    for _position_key, pt_token, remaining, sy_cost, usd_cost in store.iter_open_pt_lots():
        key = canonical_symbol(pt_token)
        prev_remaining, prev_sy, prev_usd, display = totals.get(
            key, (Decimal("0"), Decimal("0"), Decimal("0"), pt_token)
        )
        sy_total = None if (prev_sy is None or sy_cost is None) else prev_sy + sy_cost
        usd_total = None if (prev_usd is None or usd_cost is None) else prev_usd + usd_cost
        totals[key] = (prev_remaining + remaining, sy_total, usd_total, display)
    return totals


def _pt_unmeasured_row(
    symbol: str,
    chain: str,
    remaining: Decimal,
    sy_cost: Decimal | None,
    *,
    reason: str,
) -> PositionValue:
    """A held PT whose gateway USD price is unmeasured (Empty ≠ Zero, VIB-5316).

    The row STILL surfaces the MEASURED qty + SY cost basis (from the ledger),
    but the USD mark + cost + unrealized PnL are UNMEASURED. ``PositionValue``
    requires ``Decimal`` numeric fields, so they carry a placeholder
    ``Decimal("0")`` PAIRED WITH ``valuation_status="no_path"`` + explicit
    ``*_unmeasured`` detail flags — which drops the WHOLE snapshot confidence to
    ``UNAVAILABLE`` (``_determine_value_confidence``). No reader trusts the 0:
    it is never a fabricated measured-zero (spine §3.3). The value_usd 0 also
    keeps it out of ``total_value_usd`` (the ``value_usd > 0`` sum) so an
    unmeasured PT never books phantom NAV.
    """
    return PositionValue(
        position_type=PositionType.TOKEN,
        protocol="pt",
        chain=chain,
        value_usd=Decimal("0"),
        label=f"PT inventory {symbol}",
        tokens=[symbol],
        details={
            "asset": symbol,
            "source": _PT_INVENTORY_SOURCE,
            "classification": "deployed_inventory",
            "pt_symbol": symbol,
            "quantity": str(remaining),
            "sy_cost": str(sy_cost) if sy_cost is not None else "",
            "valuation_status": "no_path",
            "mark_unmeasured": True,
            "cost_basis_unmeasured": True,
            "unrealized_pnl_unmeasured": True,
            "unavailable_reason": reason,
        },
        cost_basis_usd=Decimal("0"),
        unrealized_pnl_usd=Decimal("0"),
    )


def _classify_pt_inventory(
    lot_totals: dict[str, tuple[Decimal, Decimal | None, Decimal | None, str]],
    market: "MarketDataSource",
    chain: str,
    skip_symbols: set[str],
) -> _PtInventoryClassification:
    """Value per-symbol open held-PT inventory via the gateway price (VIB-5316).

    Per symbol the result is whole-or-nothing with explicit, never-silent states:

    * ``reported_position_present`` — a discovered PT position already represents
      this symbol (VIB-5313 reprice path); FIFO defers to it (no double-count).
    * measured price (``AVAILABLE`` / ``ESTIMATED`` / ``STALE``) → a valued row;
      ESTIMATED/STALE stamps ``valuation_status="estimated"`` so the snapshot
      degrades (never folded into a HIGH NAV — spine §3.4).
    * unmeasured price (``UNAVAILABLE``) → :func:`_pt_unmeasured_row`: qty + SY
      cost shown, USD mark/cost/PnL unmeasured, snapshot → ``UNAVAILABLE``.

    USD cost-basis convention (documented, VIB-5316): the USD cost basis is the
    BUY-TIME-anchored cost aggregated from the FIFO lots
    (``cost_per_pt × remaining × underlying_price_at_buy``, summed). The mark is
    the CURRENT gateway PT/USD value; the cost is what was paid. Unrealized PnL is
    therefore the honest total return (PT-vs-par yield drift AND underlying spot
    drift), not just the PT-vs-par drift a current-price re-mark would show. When
    any contributing lot lacks a measured buy-time price (pre-fix persisted lots,
    or ``price_inputs_json`` missing the base token at buy) the USD cost is
    unmeasured (``None``) and the mark still stands — the cost is NEVER re-marked
    at the current underlying price (that re-mark sign-flipped unrealized PnL for
    volatile underlyings: the bug this fix removes).
    """
    if not lot_totals:
        return _NO_PT_INVENTORY

    pt_price_fn = getattr(market, "pt_price", None)
    if not callable(pt_price_fn):
        # Market surface predates pt_price (bare test double / data-layer
        # snapshot) → cannot value PT inventory. Stamp an explicit unmeasured
        # status, never a silent no-op.
        return _PtInventoryClassification([], {"status": "unavailable", "reason": "no_pt_price_surface"})

    from almanak.connectors._strategy_base.principal_token_valuation import value_principal_token_position
    from almanak.framework.accounting.basis import canonical_pt_symbol

    rows: list[PositionValue] = []
    token_detail: dict[str, dict[str, Any]] = {}
    skipped: dict[str, str] = {}
    measured_count = 0

    for canonical in sorted(lot_totals):
        # VIB-5355: ``canonical`` is the maturity-BEARING aggregation/pricing key;
        # ``skip_symbols`` are maturity-LESS (``canonical_pt_symbol``). Fold this
        # key through the same helper so a maturity-bearing FIFO lot is correctly
        # deduped against a maturity-less discovered position (no double-count).
        if canonical_pt_symbol(canonical) in skip_symbols:
            skipped[canonical] = "reported_position_present"
            continue
        remaining, sy_cost, buy_time_usd_cost, symbol = lot_totals[canonical]

        # VIB-5316 unpriceable-shape guard: the no-double-count safety relies on PT
        # symbols being KNOWN_UNPRICEABLE in the spot oracle (the ``PT-`` prefix), so
        # the wallet/position valuation never ALSO prices them. ``canonical`` is the
        # upper-cased symbol; if it ever loses the ``PT-`` shape (a future symbol-format
        # change), a PT inventory row would silently double-count against a wallet
        # TOKEN row. Skip + stamp rather than book a phantom — never a silent no-op.
        if not canonical.startswith("PT-"):
            logger.warning(
                "PT inventory: symbol %s entered the PT path without a PT- shape; skipping to "
                "avoid double-counting against a wallet TOKEN row",
                symbol,
            )
            skipped[canonical] = "not_pt_shape"
            continue

        try:
            pt_price = pt_price_fn(symbol, chain)
        except Exception as e:  # noqa: BLE001 — fail closed to unmeasured, never crash the snapshot
            logger.warning("PT inventory: pt_price(%s) failed (%s); marking unmeasured", symbol, e)
            rows.append(_pt_unmeasured_row(symbol, chain, remaining, sy_cost, reason="pt_price_error"))
            continue

        valued = value_principal_token_position(pt_price=pt_price, pt_amount=remaining)
        underlying = pt_price.underlying_price
        # VIB-5316: cost is the BUY-TIME-anchored USD cost from the FIFO lots — NOT
        # ``sy_cost × current_underlying`` (the re-mark that sign-flipped PnL). When
        # any contributing lot lacked a measured buy price ``buy_time_usd_cost`` is
        # None → cost/PnL honestly unmeasured while the mark still stands.
        cost_usd = buy_time_usd_cost

        if valued.current_value_usd is None or valued.confidence == ValueConfidence.UNAVAILABLE:
            rows.append(_pt_unmeasured_row(symbol, chain, remaining, sy_cost, reason="price_unmeasured"))
            continue

        measured_count += 1
        mark = valued.current_value_usd
        cost_basis = cost_usd if cost_usd is not None else Decimal("0")
        unrealized = (mark - cost_usd) if cost_usd is not None else Decimal("0")

        details: dict[str, Any] = {
            "asset": symbol,
            "source": _PT_INVENTORY_SOURCE,
            "classification": "deployed_inventory",
            "pt_symbol": symbol,
            "quantity": str(remaining),
            "sy_cost": str(sy_cost) if sy_cost is not None else "",
            "underlying_price_usd": str(underlying) if underlying is not None else "",
            "pt_to_asset_rate": str(pt_price.pt_to_asset_rate) if pt_price.pt_to_asset_rate is not None else "",
            "days_to_maturity": valued.days_to_maturity,
            "price_confidence": str(valued.confidence),
            "price_source": pt_price.source,
        }
        if cost_usd is None:
            # SY cost is measured (shown) but the underlying/USD conversion leg is
            # missing → the USD cost basis + unrealized PnL are UNMEASURED, not
            # zero. The placeholder 0 is paired with explicit flags (Empty ≠ Zero).
            details["cost_basis_unmeasured"] = True
            details["unrealized_pnl_unmeasured"] = True
        if valued.confidence != ValueConfidence.HIGH:
            details["valuation_status"] = "estimated"

        token_detail[symbol] = {
            "quantity": str(remaining),
            "value_usd": str(mark),
            "cost_usd": str(cost_usd) if cost_usd is not None else "",
            "confidence": str(valued.confidence),
        }
        rows.append(
            PositionValue(
                position_type=PositionType.TOKEN,
                protocol="pt",
                chain=chain,
                value_usd=mark,
                label=f"PT inventory {symbol}",
                tokens=[symbol],
                details=details,
                cost_basis_usd=cost_basis,
                unrealized_pnl_usd=unrealized,
            )
        )

    metadata: dict[str, Any] = {"status": "applied" if measured_count else "unmeasured"}
    if token_detail:
        metadata["tokens"] = token_detail
    if skipped:
        metadata["skipped"] = skipped
    return _PtInventoryClassification(rows, metadata)


@runtime_checkable
class MarketDataSource(Protocol):
    """Minimal interface for fetching prices and balances.

    Satisfied by both the strategy-facing MarketSnapshot and
    the data-layer MarketSnapshot.
    """

    def price(self, token: str, quote: str = "USD") -> Decimal: ...
    def balance(self, token: str) -> Any: ...


@runtime_checkable
class StrategyLike(Protocol):
    """Minimal strategy interface for PortfolioValuer."""

    @property
    def deployment_id(self) -> str: ...

    @property
    def chain(self) -> str: ...

    @property
    def wallet_address(self) -> str: ...

    def _get_tracked_tokens(self) -> list[str]: ...


def _normalize_protocol_for_dedup(protocol: str | None) -> str:
    """Normalise a protocol identifier for position-dedup identity keys.

    Collapses known lending-fork AND perp aliases onto their registry-canonical
    key (e.g. lending ``"aave"`` -> ``"aave_v3"``; perp ``"pancakeswap_perps"``
    -> ``"aster_perps"``) so a strategy-reported alias and a discovery-stamped
    canonical name dedup as ONE position instead of double-counting. A
    strategy-reported ``pancakeswap_perps`` and a discovery-stamped
    ``aster_perps`` for the same venue would otherwise key distinctly and
    survive as two positions. Protocols with no lending- or perps-read canonical
    form (LP / vault) pass through lowercased — preserving existing keying for
    every other position type.
    """
    canonical = LendingReadRegistry.canonical(protocol) or PerpsReadRegistry.canonical(protocol)
    if canonical:
        return canonical
    # canonical() already rejected None / non-str, but a *truthy* non-str
    # protocol (loosely typed PositionInfo.protocol) must still degrade safely
    # rather than crash on ``.lower()``.
    return protocol.lower() if isinstance(protocol, str) else ""


class PortfolioValuer:
    """Framework-owned portfolio valuation engine.

    Replaces strategy-level get_portfolio_snapshot() as the primary
    valuation path. Strategies still implement get_open_positions()
    for position discovery (LP, lending, perps), but the valuer
    owns the math and re-prices via gateway data.

    Usage:
        valuer = PortfolioValuer()
        snapshot = valuer.value(strategy, market)

        # With gateway client for on-chain LP re-pricing:
        valuer = PortfolioValuer(gateway_client=client)
    """

    def __init__(self, gateway_client: object | None = None) -> None:
        """Initialize the valuer.

        Args:
            gateway_client: Optional GatewayClient for on-chain LP position
                queries. If None, LP positions use strategy-reported values.
                Can also be set later via set_gateway_client().
        """
        self._gateway_client = gateway_client
        self._lp_reader = LPPositionReader(gateway_client)
        self._lending_reader = LendingPositionReader(gateway_client)
        self._perps_reader = PerpsPositionReader.from_gateway_client(gateway_client)
        self._vault_reader = VaultPositionReader(gateway_client)
        # VIB-5032: fungible two-token LP (Fluid SmartLending) — share balance →
        # per-share (token0, token1) via the connector resolver. The V3-NFT LP
        # reader cannot value a share-balance position.
        self._fungible_lp_reader = FungibleLpPositionReader(gateway_client)
        self._discovery = PositionDiscoveryService(gateway_client)
        # VIB-3424: per-position PnL enrichment from accounting_events store.
        self._accounting_store: Any = None
        self._deployment_id: str = ""
        # VIB-3503 Part 2c: per-snapshot prefetch cache for accounting events.
        # Populated by _prefetch_accounting_events() at the top of value()
        # so per-position enrichers can filter from memory rather than
        # issuing one gRPC round trip per position. Cleared at the end
        # of value() so the next snapshot does a fresh prefetch.
        self._snapshot_event_cache: dict[str, list[dict]] | None = None
        # VIB-5057: order-preserving flat view of the same prefetch (FIFO lot
        # replay needs the global timestamp-ASC order that the keyed cache
        # loses) plus a failure flag so the swap-inventory classifier can
        # distinguish "no accounting context" (no stamp, byte-identical
        # behaviour) from "context wired but events unavailable" (explicit
        # degraded stamp — never a silent no-op).
        self._snapshot_events_flat: list[dict] | None = None
        self._snapshot_prefetch_failed: bool = False

    def set_gateway_client(self, gateway_client: object | None) -> None:
        """Update the gateway client for on-chain queries.

        Called by StrategyRunner once the gateway connection is established.
        """
        self._gateway_client = gateway_client
        self._lp_reader = LPPositionReader(gateway_client)
        self._lending_reader = LendingPositionReader(gateway_client)
        self._perps_reader = PerpsPositionReader.from_gateway_client(gateway_client)
        self._vault_reader.set_gateway_client(gateway_client)
        self._fungible_lp_reader.set_gateway_client(gateway_client)
        self._discovery.set_gateway_client(gateway_client)

    def set_accounting_context(self, store: Any, deployment_id: str) -> None:
        """Set the accounting store for per-position PnL enrichment (VIB-3424).

        Called by runner_state._value_via_portfolio_valuer before value() so each
        PositionValue gets cost_basis_usd / unrealized_pnl_usd / realized_pnl_usd
        populated from the local accounting_events SQLite table.

        Args:
            store: Object with get_accounting_events_sync(deployment_id, position_key).
            deployment_id: Canonical deployment key (hosted: ALMANAK_DEPLOYMENT_ID;
                local: wallet+chain hash).
        """
        self._accounting_store = store
        self._deployment_id = deployment_id

    def value(
        self,
        strategy: StrategyLike,
        market: MarketDataSource,
        iteration_number: int = 0,
    ) -> PortfolioSnapshot:
        """Produce a PortfolioSnapshot with real USD values.

        Never raises -- returns UNAVAILABLE confidence on total failure.
        This guarantees gap-free time series for PnL charts.

        Args:
            strategy: Strategy instance for position discovery and config
            market: MarketSnapshot for price/balance queries
            iteration_number: Current strategy iteration count

        Returns:
            PortfolioSnapshot with real values and appropriate ValueConfidence
        """
        now = datetime.now(UTC)
        deployment_id = ""
        chain = ""

        # VIB-3503 Part 2c: prefetch accounting events once per snapshot so
        # the per-position enrichers below filter from memory instead of
        # issuing one gRPC round trip per position. Safe under any error
        # (cache becomes None, enrichers fall back to empty / no-op).
        # The outer try/finally guarantees the cache is cleared regardless
        # of which exit path the value() body takes, so the next snapshot
        # always starts with a fresh prefetch.
        self._prefetch_accounting_events(self._deployment_id)

        try:
            deployment_id = strategy.deployment_id
            chain = strategy.chain

            # Step 1: Discover tracked tokens from strategy config
            tracked_tokens = strategy._get_tracked_tokens()

            # Step 2: Fetch wallet balances and prices via gateway
            balances: dict[str, Decimal] = {}
            prices: dict[str, Decimal] = {}
            wallet_data_incomplete = False

            for token in tracked_tokens:
                try:
                    balance_result = market.balance(token)
                    # MarketSnapshot.balance() returns TokenBalance or Decimal
                    if hasattr(balance_result, "balance"):
                        bal = balance_result.balance
                    else:
                        bal = Decimal(str(balance_result))
                    if bal > 0:
                        balances[token] = bal
                except Exception:
                    wallet_data_incomplete = True
                    logger.debug("Could not fetch balance for %s", token)

                try:
                    price = market.price(token)
                    prices[token] = Decimal(str(price))
                except Exception:
                    if token in balances:
                        wallet_data_incomplete = True
                    logger.debug("Could not fetch price for %s", token)

            # VIB-4225 ACC-02 — append the chain's NATIVE gas-token to the
            # wallet so wallet-method PnL captures gas spend (G6 reconciliation).
            # The strategy stays fail-open at this layer — typed status lands on
            # snapshot.snapshot_metadata after construction below; runner-level
            # ``_enforce_native_gas_status_in_live`` then halts in live mode if
            # the status is non-ok / non-already_tracked.
            gas_native_status, native_row = self._resolve_native_gas(chain or "", market, balances, prices)

            # pr-auditor finding #4: when the gas helper reports a non-success
            # status, the snapshot's value_confidence MUST drop to ESTIMATED
            # rather than HIGH — otherwise dashboards trust a row whose typed
            # status says it's degraded.
            if gas_native_status not in ("ok", "already_tracked"):
                wallet_data_incomplete = True

            # Check for tokens with positive balance but missing/non-positive price
            for token in balances:
                token_price = prices.get(token)
                if token_price is None or token_price <= 0:
                    wallet_data_incomplete = True

            # Step 3: Apply spot valuation math (pure, deterministic)
            wallet_balances = value_tokens(balances, prices)
            # Append the native row directly — value_tokens filters
            # ``balance <= 0`` so a measured-zero native would otherwise be
            # silently dropped and re-introduce empty-vs-zero ambiguity.
            if native_row is not None and not any(tb.symbol == native_row.symbol for tb in wallet_balances):
                wallet_balances.append(native_row)
            wallet_value = total_value(wallet_balances)

            # Step 4: Get non-wallet positions (LP, lending, perps) if available
            positions, position_value, positions_unavailable = self._get_positions(strategy, market, prices)

            # Step 4a (VIB-5057): classify open FIFO swap-inventory lots as
            # DEPLOYED capital. The synthetic rows join ``positions`` before
            # the deployed-capital sum (their ``cost_basis_usd`` flows into it)
            # and before the value sums below; their value is subtracted from
            # ``available_cash_usd`` so wallet NAV stays invariant — only the
            # cash/deployed split moves. Zero open lots ⇒ empty rows + None
            # metadata ⇒ byte-identical snapshot to the pre-VIB-5057 writer.
            swap_inventory = self._swap_inventory_for_snapshot(chain, balances, prices)
            positions = [*positions, *swap_inventory.rows]

            # Step 4a-bis (VIB-5316): synthesize the held-PT inventory from FIFO
            # basis lots and value it via the gateway PT/USD authority — the LIVE
            # consumer that makes the VIB-5313 valuation reach NAV (a held PT is
            # not a position_event, so nothing else feeds it). PT is dropped from
            # the wallet token valuation (KNOWN_UNPRICEABLE), so these are ordinary
            # NON-wallet positions: they flow into total_value_usd /
            # wallet_total_value_usd / deployed_capital_usd through the existing
            # sums (they don't overlap the wallet index) and are NOT subtracted
            # from available_cash. Symbols already covered by a discovered PT
            # position (the VIB-5313 reprice path) are skipped — no double-count.
            # Zero open PT lots ⇒ empty rows + None metadata ⇒ byte-identical
            # snapshot to the pre-VIB-5316 writer.
            pt_inventory = self._pt_inventory_for_snapshot(chain, market, _reported_pt_symbols(positions))
            positions = [*positions, *pt_inventory.rows]

            # Step 4b: Compute deployed capital = sum of per-position cost bases.
            # cost_basis_usd is populated by _enrich_position_pnl() inside
            # _get_positions() when accounting events exist.  Only positive cost bases
            # are summed (abs guard avoids double-counting BORROW liabilities when both
            # a SUPPLY and a BORROW exist for the same asset).
            deployed_capital_usd = sum(
                (abs(p.cost_basis_usd) for p in positions if p.cost_basis_usd != Decimal("0")),
                Decimal("0"),
            )

            # Step 5: Determine confidence level
            confidence = self._determine_value_confidence(
                positions=positions,
                wallet_balances=wallet_balances,
                positions_unavailable=positions_unavailable,
                wallet_data_incomplete=wallet_data_incomplete,
            )

            # Step 6: Build audit-safe token price map (chain:address keyed)
            token_price_records = self._build_token_price_records(chain, prices, tracked_tokens)

            # VIB-4909: ``PositionType.TOKEN`` is sometimes a wallet
            # pseudo-position (SWAP-class strategies surface a tracked wallet
            # token as a TOKEN "position" for teardown enumeration + operator
            # visibility — its value already lives in ``wallet_balances``) and
            # sometimes a deployed holding NOT represented in wallet_balances
            # (e.g. ``metamorpho_eth_yield`` surfacing vault shares while the
            # wallet tracks the deposit token). The matcher classifies by wallet
            # overlap; the position stays in ``positions`` either way (operator
            # visibility) and only the value aggregations below exclude wallet
            # pseudo-positions. See PositionType.TOKEN docstring for the rules.
            wallet_index = _build_wallet_match_index(wallet_balances)

            # VIB-3614: total_value_usd is strategy-scoped (positive *deployed*
            # position values). VIB-4909 fixed wallet_total_value_usd but a
            # wallet pseudo-position still leaked into this sum, so the dashboard
            # "Wallet NAV now" tile (compute_pnl_summary:
            # ``total_value_usd + available_cash_usd``) plus the drawdown and
            # lifetime-PnL paths double-counted the wallet token — the same
            # defect VIB-4909 cured for wallet_total_value_usd, in a sibling
            # field its consumer audit did not cover. Exclude wallet
            # pseudo-positions here too so total_value_usd is truly deployed-only
            # and those PnL paths stop double-counting (VIB-4909 AC: "no silent
            # double-count for any PnL-consuming path").
            # VIB-5057: lot-derived inventory rows overlap the wallet by
            # construction (they ARE wallet-held tokens) but represent
            # DEPLOYED strategy capital — they count into total_value_usd
            # (open-position NAV) while their value is subtracted from
            # available_cash_usd, keeping NAV invariant.
            position_value_positive = sum(
                (
                    p.value_usd
                    for p in positions
                    if p.value_usd > 0
                    and (
                        _is_swap_inventory_row(p)
                        or not (p.position_type == PositionType.TOKEN and _token_overlaps_wallet_index(p, wallet_index))
                    )
                ),
                Decimal("0"),
            )

            # VIB-4909: ``wallet_total_value_usd`` is the operator-facing
            # full-portfolio value (wallet + real protocol positions); wallet
            # pseudo-positions are already counted once in ``wallet_value``.
            # VIB-5057: lot-derived inventory rows are likewise wallet-held —
            # excluded here so the full-portfolio value counts them once.
            non_wallet_position_value = sum(
                (
                    p.value_usd
                    for p in positions
                    if not _is_swap_inventory_row(p)
                    and not (p.position_type == PositionType.TOKEN and _token_overlaps_wallet_index(p, wallet_index))
                ),
                Decimal("0"),
            )

            framework_snapshot = PortfolioSnapshot(
                timestamp=now,
                deployment_id=deployment_id,
                total_value_usd=position_value_positive,
                available_cash_usd=self._idle_cash_after_inventory(wallet_value, swap_inventory.inventory_value_usd),
                deployed_capital_usd=deployed_capital_usd,
                wallet_total_value_usd=wallet_value + non_wallet_position_value,
                value_confidence=confidence,
                positions=positions,
                wallet_balances=wallet_balances,
                token_prices=token_price_records,
                chain=chain,
                iteration_number=iteration_number,
                snapshot_metadata=self._build_snapshot_metadata(
                    gas_native_status, swap_inventory.metadata, pt_inventory.metadata
                ),
            )
            # Reconciliation is advisory — never let it downgrade the framework snapshot.
            try:
                return self._reconcile_with_external(strategy, framework_snapshot)
            except Exception as recon_err:
                logger.warning("External reconciliation failed (returning framework snapshot): %s", recon_err)
                return framework_snapshot

        except Exception as e:
            # Failure contract: NEVER skip a snapshot. Persist with UNAVAILABLE.
            logger.warning("Portfolio valuation failed: %s", e)
            return PortfolioSnapshot(
                timestamp=now,
                deployment_id=deployment_id,
                total_value_usd=Decimal("0"),
                available_cash_usd=Decimal("0"),
                value_confidence=ValueConfidence.UNAVAILABLE,
                error=str(e),
                chain=chain,
                iteration_number=iteration_number,
            )
        finally:
            # Always drop the per-snapshot cache so the next value() call
            # does a fresh prefetch and never serves stale events.
            self._snapshot_event_cache = None
            self._snapshot_events_flat = None
            self._snapshot_prefetch_failed = False

    @staticmethod
    def _determine_value_confidence(
        *,
        positions: list[PositionValue],
        wallet_balances: list,
        positions_unavailable: bool,
        wallet_data_incomplete: bool,
    ) -> ValueConfidence:
        """Compute the snapshot-level confidence from per-position + wallet signals.

        VIB-4584 / F3.1 — if any position couldn't be valued through a
        registered protocol path (e.g. Aerodrome CL, Uniswap V4 LP),
        the snapshot is UNAVAILABLE — a reader cannot distinguish
        "measured zero" from "we have no idea" without this signal. Take
        precedence over ESTIMATED because one unvalued LP can hide
        arbitrary value behind a $0 row.

        Extracted from ``value()`` (Phase 5 of the snapshot pipeline) so
        the confidence policy lives in one place and ``value()`` stays
        under the CC threshold.
        """
        if any(p.details.get("valuation_status") == "no_path" for p in positions):
            return ValueConfidence.UNAVAILABLE
        has_any_value = bool(wallet_balances or positions)
        if not has_any_value and (positions_unavailable or wallet_data_incomplete):
            return ValueConfidence.UNAVAILABLE
        if positions_unavailable or wallet_data_incomplete:
            return ValueConfidence.ESTIMATED
        # VIB-5018 / VIB-4586 — a position valued through an approximate path
        # (e.g. Uniswap V4 LP, which reconstructs amounts from a price-ratio tick
        # rather than an authoritative pool slot0 read) downgrades the snapshot to
        # ESTIMATED. The value is real and traceable, but a reader must not treat
        # it as HIGH-confidence on-chain truth.
        if any(p.details.get("valuation_status") == "estimated" for p in positions):
            return ValueConfidence.ESTIMATED
        return ValueConfidence.HIGH

    def _prefetch_accounting_events(self, deployment_id: str) -> None:
        """Fetch all accounting events for the deployment once per snapshot.

        VIB-3503 Part 2c: in hosted mode each call to
        ``get_accounting_events_sync`` is a real gRPC round trip. Calling
        it per-position multiplied wire traffic by N positions per snapshot.
        Prefetching once at the top of ``value()`` collapses N round trips
        to 1 and the per-position enrichers filter from memory.

        Cache shape: ``{position_key: [event_dict, ...]}``. Events without
        a position_key (rare; defensive) are grouped under the empty key.

        Silently no-ops when the accounting store is missing or doesn't
        implement the sync primitive (preserves backwards compatibility
        with old StateManager backends).
        """
        self._snapshot_prefetch_failed = False
        if not self._accounting_store or not deployment_id:
            self._snapshot_event_cache = None
            self._snapshot_events_flat = None
            return
        if not hasattr(self._accounting_store, "get_accounting_events_sync"):
            self._snapshot_event_cache = None
            self._snapshot_events_flat = None
            return
        # Wrap the entire fetch + cache-build in one try/except so a backend
        # returning None, a non-iterable, or rows that aren't dicts can never
        # leak out of value() as an unhandled exception. Snapshot building is
        # the read-side hot path; we'd rather degrade to no PnL enrichment
        # than crash the snapshot.
        try:
            events = self._accounting_store.get_accounting_events_sync(deployment_id) or []
            cache: dict[str, list[dict]] = {}
            flat: list[dict] = []
            for ev in events:
                if not isinstance(ev, dict):
                    continue
                key = ev.get("position_key") or ""
                cache.setdefault(key, []).append(ev)
                flat.append(ev)
            self._snapshot_event_cache = cache
            self._snapshot_events_flat = flat
        except Exception:
            logger.debug("Accounting prefetch failed; falling back to per-position lookups", exc_info=True)
            self._snapshot_event_cache = None
            self._snapshot_events_flat = None
            # VIB-5057: remember the failure so the swap-inventory classifier
            # stamps an explicit degraded status instead of silently looking
            # like "no lots".
            self._snapshot_prefetch_failed = True

    def _events_for_position_key(self, position_key: str) -> list[dict]:
        """Return cached events for ``position_key`` or fall back to a per-position lookup.

        Falling back keeps backwards compatibility for any caller (test
        harness, ad-hoc) that invokes ``_enrich_*`` without going through
        ``value()`` (which is what does the prefetch).
        """
        if self._snapshot_event_cache is not None:
            return self._snapshot_event_cache.get(position_key, [])
        if not self._accounting_store or not self._deployment_id:
            return []
        if not hasattr(self._accounting_store, "get_accounting_events_sync"):
            return []
        try:
            return self._accounting_store.get_accounting_events_sync(self._deployment_id, position_key=position_key)
        except Exception:
            return []

    def _swap_inventory_for_snapshot(
        self,
        chain: str,
        balances: dict[str, Decimal],
        prices: dict[str, Decimal],
    ) -> _SwapInventoryClassification:
        """Classify this snapshot's open swap-inventory lots (VIB-5057).

        Reads the order-preserving event prefetch (``_snapshot_events_flat``)
        and never raises — any failure degrades to "inventory stays classified
        as cash" (the pre-fix behaviour) with an explicit ``unavailable``
        metadata stamp and a WARNING, never a silent no-op.

        Returns the no-op sentinel (no rows, no stamp) when no accounting
        context is wired — non-swap strategies and legacy callers stay
        byte-identical to the pre-VIB-5057 writer.
        """
        if self._snapshot_events_flat is None:
            if self._snapshot_prefetch_failed:
                logger.warning(
                    "Swap inventory classification skipped for %s: accounting events unavailable; "
                    "inventory remains classified as cash this snapshot",
                    self._deployment_id,
                )
                return _SwapInventoryClassification(
                    [], Decimal("0"), {"status": "unavailable", "reason": "events_fetch_failed"}
                )
            return _NO_SWAP_INVENTORY
        try:
            lot_totals = _aggregate_open_swap_lots(self._snapshot_events_flat, self._deployment_id)
            return _classify_swap_inventory(lot_totals, balances, prices, chain)
        except Exception:
            logger.warning(
                "Swap inventory classification failed for %s; inventory remains classified as cash",
                self._deployment_id,
                exc_info=True,
            )
            return _SwapInventoryClassification(
                [], Decimal("0"), {"status": "unavailable", "reason": "classification_error"}
            )

    def _pt_inventory_for_snapshot(
        self,
        chain: str,
        market: "MarketDataSource",
        skip_symbols: set[str],
    ) -> _PtInventoryClassification:
        """Classify this snapshot's open held-PT inventory (VIB-5316).

        Reads the order-preserving event prefetch (``_snapshot_events_flat``) and
        never raises — any failure degrades to "no PT inventory this snapshot"
        with an explicit ``unavailable`` metadata stamp and a WARNING, never a
        silent no-op. Returns the no-op sentinel (no rows, no stamp) when no
        accounting context is wired — non-Pendle strategies and legacy callers
        stay byte-identical to the pre-VIB-5316 writer.
        """
        if self._snapshot_events_flat is None:
            if self._snapshot_prefetch_failed:
                logger.warning(
                    "PT inventory classification skipped for %s: accounting events unavailable; "
                    "held PT not surfaced this snapshot",
                    self._deployment_id,
                )
                return _PtInventoryClassification([], {"status": "unavailable", "reason": "events_fetch_failed"})
            return _NO_PT_INVENTORY
        try:
            lot_totals = _aggregate_open_pt_lots(self._snapshot_events_flat, self._deployment_id)
            return _classify_pt_inventory(lot_totals, market, chain, skip_symbols)
        except Exception:
            logger.warning(
                "PT inventory classification failed for %s; held PT not surfaced this snapshot",
                self._deployment_id,
                exc_info=True,
            )
            return _PtInventoryClassification([], {"status": "unavailable", "reason": "classification_error"})

    @staticmethod
    def _idle_cash_after_inventory(wallet_value: Decimal, inventory_value_usd: Decimal) -> Decimal:
        """``available_cash_usd`` = wallet value − deployed swap inventory.

        Non-negative by construction (per token the inventory is capped at the
        wallet's actual holding, valued at the same price as the wallet sum);
        the clamp is a defensive last line so a future drift can never persist
        negative cash to the dashboard.
        """
        idle = wallet_value - inventory_value_usd
        if idle < 0:
            logger.warning(
                "available_cash_usd computed negative (wallet=%s inventory=%s); clamping to 0",
                wallet_value,
                inventory_value_usd,
            )
            return Decimal("0")
        return idle

    @staticmethod
    def _build_snapshot_metadata(
        gas_native_status: str,
        swap_inventory_metadata: dict[str, Any] | None,
        pt_inventory_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Assemble the writer-side ``snapshot_metadata`` dict.

        The ``swap_inventory`` / ``pt_inventory`` stamps are only present when
        there is something to report (applied / unmeasured / unavailable) — their
        ABSENCE is the documented "no open lots, nothing reclassified" state,
        keeping non-swap / non-Pendle strategies byte-identical (VIB-5057 /
        VIB-5316).
        """
        metadata: dict[str, Any] = {"gas_native_status": gas_native_status}
        if swap_inventory_metadata is not None:
            metadata["swap_inventory"] = swap_inventory_metadata
        if pt_inventory_metadata is not None:
            metadata["pt_inventory"] = pt_inventory_metadata
        return metadata

    def _reconcile_with_external(
        self,
        strategy: StrategyLike,
        framework_snapshot: PortfolioSnapshot,
    ) -> PortfolioSnapshot:
        """Reconcile framework valuation with external wallet portfolio data."""
        external = self._fetch_external_portfolio(strategy)
        if external is None:
            return framework_snapshot

        external_total = external["total_value_usd"]
        # VIB-3614: total_value_usd is position-scoped; use wallet_total_value_usd
        # (which mirrors the pre-VIB-3614 full-wallet value) for the divergence
        # comparison so that wallet-only strategies still reconcile correctly.
        framework_total = framework_snapshot.wallet_total_value_usd

        # VIB-4225: preserve any pre-existing typed status (e.g.
        # gas_native_status set by _resolve_native_gas) by merging the new
        # reconciliation metadata into the existing dict rather than wholesale
        # replacing it. Without this merge, the runner-level enforcer at
        # _enforce_native_gas_status_in_live would never see the stamp.
        metadata = dict(framework_snapshot.snapshot_metadata or {})
        metadata.update(
            {
                "valuation_source": "framework",
                "external_provider": external["provider"],
                "framework_total_value_usd": str(framework_total),
                "external_total_value_usd": str(external_total),
                "reconciliation_status": "framework_only",
                "external_cache_hit": external["cache_hit"],
                "external_timestamp": external["timestamp"].isoformat(),
                "external_positions_count": len(external["positions"]),
            }
        )

        if external_total <= 0:
            metadata["reconciliation_status"] = "external_non_positive"
            framework_snapshot.snapshot_metadata = metadata
            return framework_snapshot

        divergence_ratio = self._calculate_divergence_ratio(framework_total, external_total)
        metadata["divergence_ratio"] = str(divergence_ratio)

        # External only replaces framework when framework reports zero.
        # When both are positive, framework's on-chain queries are authoritative;
        # external data is advisory metadata for operator dashboards.
        if framework_total <= 0 and external_total > 0:
            metadata["valuation_source"] = "reconciled_external"
            metadata["reconciliation_status"] = "external_won_zero_framework"
            logger.warning(
                "External portfolio valuation replaced zero framework value for %s on %s: framework=$%s external=$%s",
                framework_snapshot.deployment_id,
                framework_snapshot.chain,
                framework_total,
                external_total,
            )
            return self._build_external_reconciled_snapshot(framework_snapshot, external, metadata)

        if framework_total > 0 and divergence_ratio <= FRAMEWORK_EXTERNAL_AGREEMENT_THRESHOLD:
            metadata["reconciliation_status"] = "framework_won_close_agreement"
        elif divergence_ratio > FRAMEWORK_EXTERNAL_DIVERGENCE_THRESHOLD:
            metadata["reconciliation_status"] = "framework_won_large_divergence"
            logger.warning(
                "Large divergence between framework and external for %s on %s: framework=$%s external=$%s divergence=%s",
                framework_snapshot.deployment_id,
                framework_snapshot.chain,
                framework_total,
                external_total,
                divergence_ratio,
            )
        else:
            metadata["reconciliation_status"] = "framework_won_moderate_divergence"

        framework_snapshot.snapshot_metadata = metadata
        return framework_snapshot

    def _fetch_external_portfolio(self, strategy: StrategyLike) -> dict[str, Any] | None:
        """Fetch external wallet portfolio data through the gateway integration RPC."""
        gateway_client = self._gateway_client
        if gateway_client is None:
            return None

        wallet_address = getattr(strategy, "wallet_address", "")
        chain = getattr(strategy, "chain", "")
        if not wallet_address or not chain:
            return None

        try:
            from almanak.gateway.proto import gateway_pb2

            response = gateway_client.integration.GetWalletPortfolio(  # type: ignore[attr-defined]
                gateway_pb2.WalletPortfolioRequest(wallet_address=wallet_address, chain=chain)
            )
        except Exception as e:
            logger.debug("External portfolio RPC failed for %s on %s: %s", wallet_address, chain, e)
            return None

        if not response.success:
            logger.debug(
                "External portfolio unavailable for %s on %s: %s",
                wallet_address,
                chain,
                response.error or "unknown error",
            )
            return None

        try:
            total_value_usd = Decimal(response.total_value_usd or "0")
        except Exception:
            logger.debug("Invalid total_value_usd from external portfolio: %r", response.total_value_usd)
            return None

        return {
            "provider": response.provider or "unknown",
            "total_value_usd": total_value_usd,
            "cache_hit": bool(response.cache_hit),
            "timestamp": datetime.fromtimestamp(response.timestamp, tz=UTC)
            if response.timestamp
            else datetime.now(UTC),
            "positions": [self._external_position_to_value(position, chain) for position in response.positions],
        }

    def _build_external_reconciled_snapshot(
        self,
        framework_snapshot: PortfolioSnapshot,
        external: dict[str, Any],
        metadata: dict[str, Any],
    ) -> PortfolioSnapshot:
        """Build a reconciled snapshot where the external total wins."""
        external_positions = external["positions"]
        merged_positions = self._merge_external_positions(framework_snapshot.positions, external_positions)
        external_total = external["total_value_usd"]

        # VIB-3614: use position-scoped sum as total_value_usd, consistent with
        # the framework path.  external_total (full wallet from Zerion) goes to
        # wallet_total_value_usd for operator debugging.
        pos_total = sum((p.value_usd for p in merged_positions if p.value_usd > 0), Decimal("0"))
        gross_position_total = sum((p.value_usd for p in merged_positions), Decimal("0"))
        available_cash_usd = max(Decimal("0"), external_total - gross_position_total)

        # VIB-4584 / F3.1: preserve UNAVAILABLE when the framework couldn't
        # value at least one position through a registered path. External
        # portfolio totals are advisory — they cannot retroactively certify
        # a value the framework never verified. Without this guard, an
        # external Zerion read would silently downgrade UNAVAILABLE →
        # ESTIMATED and hide the data-quality signal F3.1 was added to
        # surface.
        reconciled_confidence = (
            ValueConfidence.UNAVAILABLE
            if framework_snapshot.value_confidence == ValueConfidence.UNAVAILABLE
            else ValueConfidence.ESTIMATED
        )

        return PortfolioSnapshot(
            timestamp=framework_snapshot.timestamp,
            deployment_id=framework_snapshot.deployment_id,
            total_value_usd=pos_total,
            available_cash_usd=available_cash_usd,
            deployed_capital_usd=framework_snapshot.deployed_capital_usd,
            wallet_total_value_usd=external_total,
            value_confidence=reconciled_confidence,
            error=framework_snapshot.error,
            positions=merged_positions,
            wallet_balances=framework_snapshot.wallet_balances,
            chain=framework_snapshot.chain,
            iteration_number=framework_snapshot.iteration_number,
            snapshot_metadata=metadata,
        )

    def _external_position_to_value(self, position: Any, chain: str) -> PositionValue:
        """Convert external portfolio data into a PositionValue."""
        details = self._decode_external_details(position.raw_details_json)
        pool_address = getattr(position, "pool_address", "") or ""
        if pool_address:
            details.setdefault("pool_address", pool_address)
        details.setdefault("position_id", getattr(position, "position_id", ""))
        details.setdefault("source", "external_portfolio_api")

        return PositionValue(
            position_type=self._map_external_position_type(getattr(position, "position_type", "")),
            protocol=getattr(position, "protocol", "unknown") or "unknown",
            chain=chain,
            value_usd=Decimal(getattr(position, "value_usd", "0") or "0"),
            label=getattr(position, "label", "") or getattr(position, "protocol", "external"),
            tokens=list(getattr(position, "token_symbols", []) or []),
            details=details,
        )

    def _merge_external_positions(
        self,
        framework_positions: list[PositionValue],
        external_positions: list[PositionValue],
    ) -> list[PositionValue]:
        """Merge framework and external positions, preserving framework detail when possible."""
        merged = list(framework_positions)

        for external_position in external_positions:
            match_index = next(
                (index for index, existing in enumerate(merged) if self._positions_match(existing, external_position)),
                None,
            )
            if match_index is None:
                merged.append(external_position)
                continue

            existing = merged[match_index]
            merged[match_index] = PositionValue(
                position_type=existing.position_type or external_position.position_type,
                protocol=existing.protocol or external_position.protocol,
                chain=existing.chain or external_position.chain,
                value_usd=external_position.value_usd,
                label=existing.label or external_position.label,
                tokens=existing.tokens or external_position.tokens,
                details={**external_position.details, **existing.details},
            )

        return merged

    @staticmethod
    def _positions_match(existing: PositionValue, external_position: PositionValue) -> bool:
        """Determine whether framework and external positions refer to the same exposure."""
        if existing.protocol.lower() != external_position.protocol.lower():
            return False

        existing_pool = str(existing.details.get("pool_address") or existing.details.get("pool") or "").lower()
        external_pool = str(external_position.details.get("pool_address") or "").lower()
        if existing_pool and external_pool:
            return existing_pool == external_pool

        return existing.label == external_position.label and set(existing.tokens) == set(external_position.tokens)

    @staticmethod
    def _decode_external_details(raw_details_json: str) -> dict[str, Any]:
        """Decode external raw details JSON defensively."""
        if not raw_details_json:
            return {}
        try:
            payload = json.loads(raw_details_json)
            return payload if isinstance(payload, dict) else {}
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def _map_external_position_type(position_type: str) -> Any:
        """Map external provider position types onto Almanak teardown position types."""
        from almanak.framework.teardown.models import PositionType

        normalized = position_type.strip().lower()
        if "perp" in normalized or "future" in normalized:
            return PositionType.PERP
        if "borrow" in normalized or "debt" in normalized or "loan" in normalized:
            return PositionType.BORROW
        if "supply" in normalized or "deposit" in normalized or "lend" in normalized:
            return PositionType.SUPPLY
        if "vault" in normalized or "yield" in normalized or "earn" in normalized:
            return PositionType.VAULT
        if "stake" in normalized or "farm" in normalized:
            return PositionType.STAKE
        if "predict" in normalized:
            return PositionType.PREDICTION
        if "cex" in normalized:
            return PositionType.CEX
        if "lp" in normalized or "liquidity" in normalized or "pool" in normalized:
            return PositionType.LP
        return PositionType.TOKEN

    @staticmethod
    def _calculate_divergence_ratio(framework_total: Decimal, external_total: Decimal) -> Decimal:
        """Return the absolute divergence ratio between framework and external totals."""
        baseline = max(abs(framework_total), abs(external_total))
        if baseline <= 0:
            return Decimal("0")
        return abs(framework_total - external_total) / baseline

    @staticmethod
    def _resolve_native_gas(
        chain: str,
        market: Any,
        balances: dict[str, Decimal],
        prices: dict[str, Decimal],
    ) -> tuple[str, TokenBalance | None]:
        """VIB-4225 ACC-02 — fold the chain's native gas-token into the wallet.

        Returns ``(status, native_row)``:

        - ``status`` is one of ``"ok"`` / ``"already_tracked"`` /
          ``"unknown_chain"`` / ``"balance_failed"`` / ``"price_missing"`` —
          the runner-level enforcer reads it off
          ``snapshot.snapshot_metadata["gas_native_status"]``.
        - ``native_row`` is a :class:`TokenBalance` for the native token (even
          when the balance is exactly ``Decimal("0")``, preserving "Empty !=
          Zero": measured zero is durable, absence means unmeasured) when the
          status is ``"ok"``. ``None`` for every other status, including
          ``"already_tracked"`` (in that case, the row was already produced
          by the upstream tracked-tokens loop).

        Mutates the ``balances`` + ``prices`` dicts only on the ``"ok"``
        path so downstream price-records / confidence calc see the native
        symbol — but the caller is responsible for appending ``native_row``
        to ``wallet_balances`` so ``value_tokens``'s ``balance <= 0`` filter
        does NOT silently drop a measured-zero native (Codex P2 #2 +
        pr-auditor finding #2).

        Strategy stays fail-open: this method NEVER raises; the live-mode
        halt is enforced at ``runner_state._enforce_native_gas_status_in_live``
        which inspects the typed status after the snapshot is built.
        """
        try:
            from almanak.framework.accounting.gas_pricing import native_token_for_chain

            native_symbol = native_token_for_chain(chain)
        except Exception as e:  # noqa: BLE001 — typed status path
            logger.debug("native gas-token chain resolve failed: %s", e)
            return ("unknown_chain", None)
        if not native_symbol:
            return ("unknown_chain", None)

        canon = native_symbol.upper()
        # Case-insensitive dedupe — a strategy that already tracks the native
        # symbol (rare) keeps its single entry.
        existing_key = next(
            (tok for tok in balances if (tok or "").upper() == canon),
            None,
        )
        if existing_key is not None:
            # Codex P2 #1: when the upstream tracked-tokens loop fetched the
            # native balance but its price lookup failed, the native is in
            # ``balances`` without a matching ``prices`` entry, and
            # ``value_tokens`` will silently drop the row. Surface the
            # mismatch as ``price_missing`` so the runner-level enforcer
            # halts (live) or stamps ERROR (paper) instead of stamping a
            # misleading "already_tracked" trail.
            existing_price = prices.get(existing_key)
            if existing_price is None or existing_price <= 0:
                return ("price_missing", None)
            return ("already_tracked", None)

        # CodeRabbit thread #11: keep the fail-open contract around
        # malformed balance/price values. A market stub that returns a
        # ``TokenBalance(balance=None)`` or a non-Decimal price object
        # would crash ``Decimal(str(...))`` and bubble up to the
        # snapshot-wide UNAVAILABLE handler, losing the typed-status
        # trail. Pulling the conversion inside the try/except keeps the
        # helper truly fail-open: malformed shape surfaces as
        # ``balance_failed`` / ``price_missing`` instead of an
        # unhandled exception.
        try:
            balance_result = market.balance(native_symbol)
            raw_balance = balance_result.balance if hasattr(balance_result, "balance") else balance_result
            bal = Decimal(str(raw_balance))
        except Exception as e:  # noqa: BLE001 — typed status path
            logger.debug("native gas-token balance fetch failed: %s", e)
            return ("balance_failed", None)

        try:
            price = market.price(native_symbol)
            if price is None:
                return ("price_missing", None)
            price_d = Decimal(str(price))
        except Exception as e:  # noqa: BLE001 — typed status path
            logger.debug("native gas-token price fetch failed: %s", e)
            return ("price_missing", None)
        # Build a TokenBalance row directly so the caller can append it to
        # ``wallet_balances`` even when ``bal == 0``. ``value_tokens``'s
        # ``balance <= 0`` filter would otherwise drop measured zero, which
        # silently re-introduces the empty-vs-zero ambiguity this PR fixes.
        native_row = TokenBalance(
            symbol=canon,
            balance=bal,
            value_usd=bal * price_d,
            price_usd=price_d,
        )
        # Mirror into balances/prices so downstream consumers (price-records
        # audit map, confidence calc) see the native symbol; positive
        # balances flow through ``value_tokens`` normally, zero balances are
        # carried by ``native_row`` instead.
        if bal > 0:
            balances[canon] = bal
        prices[canon] = price_d
        return ("ok", native_row)

    @staticmethod
    def _build_token_price_records(
        chain: str,
        prices: dict[str, Decimal],
        tracked_tokens: list[str],
    ) -> dict[str, dict]:
        """Build an audit-safe token price map keyed by chain:address.

        Each entry contains the USD price, display symbol, and decimals so
        historical snapshots can be re-verified without re-querying oracles.
        """
        token_price_records: dict[str, dict] = {}
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
        except Exception:
            resolver = None

        for token in tracked_tokens:
            price = prices.get(token)
            if price is None or price <= 0:
                continue
            try:
                if resolver:
                    resolved = resolver.resolve(token, chain)
                    address = resolved.address if resolved else token
                    decimals = resolved.decimals if resolved else None
                else:
                    address = token
                    decimals = None
                key = f"{chain}:{address.lower()}" if address.startswith("0x") else f"{chain}:{token}"
                token_price_records[key] = {
                    "price_usd": str(price),
                    "symbol": token,
                    "decimals": decimals,
                }
            except Exception:
                # Best-effort: fall back to symbol-only key
                token_price_records[f"{chain}:{token}"] = {
                    "price_usd": str(price),
                    "symbol": token,
                    "decimals": None,
                }
        return token_price_records

    def _get_positions(
        self,
        strategy: StrategyLike,
        market: MarketDataSource,
        prices: dict[str, Decimal],
    ) -> tuple[list[PositionValue], Decimal, bool]:
        """Discover and value non-wallet positions.

        Two-source strategy:
        1. **Discovery** (primary): Framework-owned PositionDiscoveryService
           scans on-chain for lending/LP positions using strategy config.
        2. **Strategy** (supplementary): strategy.get_open_positions() provides
           position types that discovery can't detect (perps, stakes, etc.)
           and LP token IDs that discovery uses for scanning.

        Positions from both sources are deduplicated by *canonical identity*
        (VIB-4838), not by raw ``position_id``. Lending discovery emits
        ``aave-{supply,borrow}-{symbol}-{chain}`` ids while strategies pick
        their own (``aave-wbtc-collateral`` etc.); raw-string dedup let a
        strategy stub and the discovery position for the *same* on-chain
        reserve both survive, poisoning ``value_confidence`` and risking a NAV
        double-count. See ``_merge_position_sources``.
        All positions are re-priced with on-chain data when possible.

        Returns:
            (positions, total_position_value, positions_unavailable)
        """
        # Source 1: Strategy-reported positions (get_open_positions)
        strategy_positions, strategy_failed = self._get_strategy_positions(strategy)

        # Source 2: Framework discovery (on-chain scanning)
        discovered_positions: list[PositionInfo] = []
        discovery_had_errors = False
        # Perp venues whose on-chain book discovery read successfully (VIB-5252):
        # for these, discovery is the complete authoritative set and a strategy's
        # notional perp stub is redundant. Empty when discovery did not run.
        perp_protocols_ok: set[str] = set()
        discovery_config = self._build_discovery_config(strategy, strategy_positions)
        if discovery_config:
            discovered = self._discovery.discover(discovery_config)
            if discovered.errors:
                discovery_had_errors = True
            discovered_positions = list(discovered.positions)
            perp_protocols_ok = set(discovered.perp_protocols_ok)

        # Merge the two sources by canonical identity (VIB-4838): discovery is
        # authoritative for value + on-chain details, the strategy for
        # position_type + domain hints. Degenerate strategy stubs that merely
        # duplicate a discovered position are dropped here so they cannot reach
        # the repricer and trip ``no_path``.
        merged_positions = self._merge_position_sources(
            strategy_positions, discovered_positions, strategy.chain, perp_protocols_ok
        )

        positions_incomplete = strategy_failed or discovery_had_errors
        if not merged_positions:
            return [], Decimal("0"), positions_incomplete

        # Re-price all positions and enrich details with valuer breakdown
        positions: list[PositionValue] = []
        any_unrepriced = False
        # Track-C lending state (HF + Comet APY base) is a per-WALLET read shared
        # by every leg of a wallet's position (a leverage loop's SUPPLY + BORROW
        # legs hit the same (protocol, chain, wallet, market_id)), so cache it per
        # snapshot. market_id is None for whole-account protocols (Aave family).
        account_state_cache: dict[tuple[str, str, str, str | None], Any] = {}
        for p in merged_positions:
            value_usd, enriched_details, repriced = self._reprice_position_enriched(p, strategy.chain, market)
            if not repriced:
                any_unrepriced = True
                # VIB-4584 / F3.1 — surface the per-position signal on the
                # serialized details so dashboards / downstream auditors can
                # filter "we couldn't value this position" without re-running
                # the valuer. The snapshot-level confidence is set below in
                # ``value()`` step 5.
                enriched_details = {**enriched_details, "valuation_status": "no_path"}
                logger.warning(
                    "No registered valuation path for %s position on protocol=%s "
                    "(position_id=%s); snapshot value_confidence will be UNAVAILABLE",
                    p.position_type.value,
                    p.protocol,
                    p.position_id,
                )

            # Stamp Track-C lending fields (health_factor + Comet supply/borrow
            # APY, cached per wallet) onto lending legs so the
            # ``position_state_snapshots`` rows carry them (Accountant L2/L3/L5).
            # No-op for non-lending positions. Guarded: enrichment fails closed
            # internally, but the snapshot must NEVER be aborted by an unforeseen
            # raise here — dropping this position's Track-C row would regress
            # coverage (G14/G15). Degrade to "no fields stamped", never "no row".
            try:
                enriched_details = self._enrich_lending_trackc_fields(
                    p,
                    strategy.chain,
                    enriched_details,
                    account_state_cache,
                    market,
                    strategy_wallet=getattr(strategy, "wallet_address", None),
                )
            except Exception:
                logger.debug(
                    "Track-C lending enrichment raised for %s; leaving fields unmeasured",
                    p.position_id,
                    exc_info=True,
                )

            # Merge enriched valuer details into position details
            merged_details = {**p.details, **enriched_details}

            pos = PositionValue(
                position_type=p.position_type,
                protocol=p.protocol,
                chain=p.chain,
                value_usd=value_usd,
                label=f"{p.protocol} {p.position_type.value}",
                tokens=p.details.get("tokens", []),
                details=merged_details,
            )
            # VIB-3424: populate cost_basis / pnl fields from accounting events
            self._enrich_position_pnl(pos, p, strategy.chain)
            positions.append(pos)

        position_value = sum((p.value_usd for p in positions), Decimal("0"))
        # Signal incomplete if strategy failed, discovery had errors, OR any
        # position couldn't be valued through a registered protocol path.
        return positions, position_value, positions_incomplete or any_unrepriced

    def _get_strategy_positions(self, strategy: StrategyLike) -> tuple[list["PositionInfo"], bool]:
        """Get positions from strategy.get_open_positions(), gracefully.

        Returns:
            (positions, failed) — failed is True if get_open_positions raised.
        """
        if not hasattr(strategy, "get_open_positions"):
            return [], False
        try:
            summary: TeardownPositionSummary = strategy.get_open_positions()
            if summary and summary.positions:
                return list(summary.positions), False
            return [], False
        except Exception as e:
            logger.warning("Failed to get open positions: %s", e)
            return [], True

    def _build_discovery_config(
        self,
        strategy: StrategyLike,
        strategy_positions: list["PositionInfo"],
    ) -> DiscoveryConfig | None:
        """Build discovery config from strategy metadata.

        Returns None if we don't have enough information to discover anything.
        """
        try:
            chain = strategy.chain
            wallet = strategy.wallet_address
            if not chain or not wallet:
                return None

            # Get protocols from strategy metadata
            protocols: list[str] = []
            metadata = getattr(strategy, "STRATEGY_METADATA", None)
            if metadata and hasattr(metadata, "supported_protocols"):
                protocols = list(metadata.supported_protocols)

            # Get tracked tokens
            tracked_tokens: list[str] = []
            try:
                tracked_tokens = strategy._get_tracked_tokens()
            except Exception:
                pass

            # Extract LP token IDs from strategy-reported positions
            lp_token_ids: list[int] = []
            lp_protocol = "uniswap_v3"
            for p in strategy_positions:
                from almanak.framework.teardown.models import PositionType as PT

                if p.position_type == PT.LP:
                    token_id = self._extract_token_id(p)
                    if token_id is not None:
                        lp_token_ids.append(token_id)
                    if p.protocol:
                        lp_protocol = p.protocol
                # Seed discovery with any perp venue the strategy itself reports
                # (VIB-5252). ``protocols`` is otherwise sourced only from
                # ``STRATEGY_METADATA.supported_protocols``; a strategy that
                # reports a perp position but omits the venue from its metadata
                # would never have its perps scanned, so the notional stub would
                # survive. Harvesting the venue here guarantees the discovery
                # scan that lets the merge drop the stub for net-equity.
                elif p.position_type == PT.PERP and p.protocol and p.protocol not in protocols:
                    protocols.append(p.protocol)

            if not protocols and not tracked_tokens:
                return None

            return DiscoveryConfig(
                chain=chain,
                wallet_address=wallet,
                protocols=protocols,
                tracked_tokens=tracked_tokens,
                lp_token_ids=lp_token_ids,
                lp_protocol=lp_protocol,
            )
        except Exception:
            logger.debug("Could not build discovery config", exc_info=True)
            return None

    def _resolve_position_asset_address(self, position: "PositionInfo", chain: str) -> str | None:
        """Best-effort underlying-asset address for a position (VIB-4838).

        Tries the on-chain address already in ``details`` first, then resolves
        the ``asset`` symbol through the token registry. Returns a lowercased
        address or ``None`` when no identity can be derived. Mirrors the
        resolution order in ``_reprice_lending_on_chain_enriched`` so the
        dedup key and the repricer agree on identity.
        """
        addr = self._extract_asset_address(position)
        if not addr:
            asset_symbol = position.details.get("asset")
            if asset_symbol:
                try:
                    from almanak.framework.data.tokens import get_token_resolver

                    resolved = get_token_resolver().resolve(asset_symbol, chain)
                    if resolved and resolved.address:
                        addr = resolved.address
                except Exception:
                    addr = None
        return addr.lower() if addr else None

    def _canonical_position_key(self, position: "PositionInfo", chain: str) -> tuple | None:
        """Identity tuple used to dedup strategy + discovery positions (VIB-4838).

        Address beats symbol for lending so ETH/WETH ambiguity and custom-token
        symbol collisions cannot collapse distinct reserves. Returns ``None``
        when no stable identity is derivable (e.g. an identity-less strategy
        stub), which forces the degenerate-stub guard to decide its fate
        instead of letting it masquerade as its own position.
        """
        from almanak.framework.teardown.models import PositionType

        chain_l = (chain or position.chain or "").lower()
        protocol_l = _normalize_protocol_for_dedup(position.protocol)

        if position.position_type in (PositionType.SUPPLY, PositionType.BORROW):
            asset_address = self._resolve_position_asset_address(position, chain)
            if not asset_address:
                return None
            return (position.position_type, protocol_l, chain_l, asset_address)

        if position.position_type == PositionType.LP:
            token_id = self._extract_token_id(position)
            if token_id is None:
                return None
            return (PositionType.LP, protocol_l, chain_l, token_id)

        if position.position_type == PositionType.PERP:
            market = position.details.get("market") or position.details.get("market_address")
            if not market:
                return None
            direction = str(position.direction or position.details.get("direction") or "").upper()
            is_long = direction in ("LONG", "BUY") or position.details.get("is_long") is True
            return (PositionType.PERP, protocol_l, chain_l, str(market).lower(), is_long)

        return None

    def _is_degenerate_stub(
        self,
        stub: "PositionInfo",
        discovery_positions: list["PositionInfo"],
        chain: str,
    ) -> bool:
        """Decide whether a strategy stub is a phantom duplicate to drop (VIB-4838).

        Drop ONLY when the stub is truly identity-less (no resolvable asset
        address, no wallet hint) AND carries no value (``value_usd == 0``) AND
        discovery returned ≥1 position for the same
        ``(protocol, position_type, chain)`` — i.e. discovery already accounts
        for whatever the stub gestured at. All three conditions are
        load-bearing.

        KEEP the stub (let it reprice / flag ``no_path`` and degrade
        confidence) when it carries an ``asset`` hint that resolves to an
        address disagreeing with every discovery position in that group — the
        disagreement is real signal, not a phantom (VIB-4584 false-positive
        guard). Canonical-key matches are handled by the merge before this is
        called, so this only sees stubs whose key is ``None``.
        """
        from almanak.framework.teardown.models import PositionType

        if stub.position_type not in (PositionType.SUPPLY, PositionType.BORROW):
            return False

        has_wallet = bool(stub.details.get("wallet") or stub.details.get("wallet_address") or stub.details.get("owner"))
        stub_address = self._resolve_position_asset_address(stub, chain)
        # Identity-bearing stub (resolvable address or wallet) is never a
        # phantom here — if it matched discovery the merge already collapsed
        # it; reaching this point means it is a distinct position.
        if stub_address or has_wallet:
            return False
        if stub.value_usd != Decimal("0"):
            return False

        protocol_l = _normalize_protocol_for_dedup(stub.protocol)
        chain_l = (chain or stub.chain or "").lower()
        same_group = [
            d
            for d in discovery_positions
            if d.position_type == stub.position_type
            and _normalize_protocol_for_dedup(d.protocol) == protocol_l
            and (chain or d.chain or "").lower() == chain_l
        ]
        return bool(same_group)

    def _merge_position_sources(
        self,
        strategy_positions: list["PositionInfo"],
        discovered_positions: list["PositionInfo"],
        chain: str,
        perp_protocols_ok: set[str] | None = None,
    ) -> list["PositionInfo"]:
        """Dedup strategy + discovery positions by canonical identity (VIB-4838).

        Discovery is authoritative for value + on-chain details; the strategy
        is authoritative for ``position_type`` + domain hints. The legacy
        merge (raw ``position_id`` keyed) kept the *strategy's* value on a
        collision, which under-valued or double-counted lending positions
        whenever the strategy's id did not byte-match discovery's internal
        scheme. Re-keying on canonical identity fixes both the
        confidence-poisoning and the latent NAV double-count described in the
        VIB-4838 brief. See ``blueprints/27-accounting.md`` §7 (Layer 2
        snapshots) and the §3.4 S9 no-double-count invariant.

        **Perps — discovery wins (VIB-5252).** A strategy reports a perp as a
        ``details["market"]``-as-SYMBOL stub whose ``value_usd`` is gross
        NOTIONAL (collateral × leverage) and which carries no wallet hint, so it
        can never reprice to net-equity. Only the *discovered* perp carries the
        on-chain market address + wallet that ``_value_matched_perp`` needs to
        compute §7.4 net-equity (collateral + uPnL − fees). The two key
        distinctly (symbol vs address), so the canonical-key collapse above
        never merges them. ``perp_protocols_ok`` (the venues discovery read
        successfully — a single account-level read returns the whole book)
        lets us resolve this: when discovery authoritatively scanned a perp
        venue, the strategy's notional stub is redundant and is dropped, and
        discovery's complete book is added below. This *inverts* the historical
        precedence (which kept the stub) while preserving its only real purpose,
        anti-double-count — discovery and stub are still never both kept. When
        discovery did NOT scan ok for the venue (read failed / venue undeclared)
        the stub is preserved, and ``IntentStrategy.get_portfolio_snapshot``'s
        degraded fallback excludes its notional rather than re-booking it.
        """
        from almanak.framework.teardown.models import PositionType as _PT

        # Venues for which a strategy's notional perp stub is redundant and must
        # be dropped (VIB-5252). A stub is redundant when discovery either (a)
        # scanned the venue successfully — ``perp_protocols_ok``, which also
        # covers the flat/empty book — or (b) already returned a perp for that
        # venue. (b) makes the anti-double-count guarantee independent of the
        # caller threading ``perp_protocols_ok``: discovery's account-level read
        # returns the complete book, so a discovered perp for a venue means any
        # same-venue stub is either that position (deduped) or an unfilled one
        # (correctly dropped). In production (a) always implies (b)'s superset.
        perp_drop_protocols = {_normalize_protocol_for_dedup(p) for p in (perp_protocols_ok or set())}
        perp_drop_protocols |= {
            _normalize_protocol_for_dedup(d.protocol) for d in discovered_positions if d.position_type == _PT.PERP
        }

        # Index discovery by canonical key (discovery wins on collision).
        discovery_by_key: dict[tuple, PositionInfo] = {}
        for d in discovered_positions:
            key = self._canonical_position_key(d, chain)
            if key is not None:
                discovery_by_key[key] = d

        merged: list[PositionInfo] = []
        consumed_discovery: set[int] = set()

        # 1. Fold strategy positions, collapsing onto discovery where keys match.
        for sp in strategy_positions:
            key = self._canonical_position_key(sp, chain)
            disc = discovery_by_key.get(key) if key is not None else None
            if disc is not None and id(disc) in consumed_discovery:
                # A prior strategy position already collapsed onto this
                # discovery row; this one is a redundant self-report.
                continue
            if disc is not None:
                # Collapse: take discovery's value + on-chain details (truth),
                # keep the strategy's position_type + domain hints.
                merged.append(
                    PositionInfo(
                        position_type=sp.position_type,
                        position_id=disc.position_id,
                        chain=disc.chain,
                        protocol=disc.protocol,
                        value_usd=disc.value_usd,  # discovery is authoritative
                        details={**sp.details, **disc.details},
                    )
                )
                consumed_discovery.add(id(disc))
                continue
            # Perp stub whose venue discovery authoritatively scanned (VIB-5252):
            # discovery is the complete on-chain book (filled positions are added
            # in step 2; an empty ok-scan means the perp is flat). The stub only
            # carries notional and cannot reprice, so drop it — never let it
            # masquerade as equity.
            if sp.position_type == _PT.PERP and _normalize_protocol_for_dedup(sp.protocol) in perp_drop_protocols:
                logger.debug(
                    "VIB-5252: dropping notional perp stub (position_id=%s) — "
                    "discovery authoritatively scanned protocol=%s on %s",
                    sp.position_id,
                    sp.protocol,
                    chain,
                )
                continue
            # No canonical match. Drop only true phantom duplicates.
            if self._is_degenerate_stub(sp, discovered_positions, chain):
                logger.debug(
                    "VIB-4838: dropping degenerate %s stub (position_id=%s) — "
                    "discovery already covers protocol=%s on %s",
                    sp.position_type.value,
                    sp.position_id,
                    sp.protocol,
                    chain,
                )
                continue
            merged.append(sp)

        # 2. Add discovery positions the strategy did not already collapse onto.
        # Discovered perps are added unconditionally (VIB-5252): the historical
        # skip that dropped a discovered perp whenever the strategy reported one
        # is gone — discovery is the authoritative net-equity source, and the
        # redundant strategy stub was already dropped in step 1.
        for d in discovered_positions:
            if id(d) in consumed_discovery:
                continue
            merged.append(d)

        return merged

    def _reprice_position(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> Decimal:
        """Re-price a single position using on-chain data when possible.

        For LP positions: query on-chain V3 data and re-calculate value.
        For SUPPLY/BORROW: query on-chain Aave data and re-calculate value.
        For VAULT: query ERC-4626 share balance + convertToAssets via the
            vault adapter registry.
        For other types: pass through strategy-reported value.

        Falls back to strategy-reported value_usd on any failure.
        """
        from almanak.framework.teardown.models import PositionType

        if position.position_type == PositionType.LP:
            repriced = self._reprice_lp_on_chain(position, chain, market)
            if repriced is not None:
                return repriced
            return position.value_usd

        if position.position_type in (PositionType.SUPPLY, PositionType.BORROW):
            repriced = self._reprice_lending_on_chain(position, chain, market)
            if repriced is not None:
                return repriced
            # Normalize fallback: BORROW should reduce portfolio (negative),
            # matching the on-chain path which returns -debt_value_usd.
            if position.position_type == PositionType.BORROW and position.value_usd > 0:
                return -position.value_usd
            return position.value_usd

        if position.position_type == PositionType.PERP:
            repriced = self._reprice_perps_on_chain(position, chain, market)
            if repriced is not None:
                return repriced
            return position.value_usd

        if position.position_type == PositionType.VAULT:
            repriced = self._reprice_vault_on_chain(position, chain, market)
            if repriced is not None:
                return repriced
            return position.value_usd

        return position.value_usd

    def _reprice_position_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any], bool]:
        """Re-price a position and return enriched details for persistence.

        Returns:
            (value_usd, enriched_details, repriced) where ``repriced`` is
            ``True`` when a protocol-specific on-chain path successfully
            valued the position (or when a strategy-authoritative fallback
            applies, e.g. PERPs/VAULTs the strategy reports directly).
            ``False`` signals that no registered valuation path matched
            the protocol — VIB-4584 / F3.1: the snapshot's
            ``value_confidence`` must drop to ``UNAVAILABLE`` so a reader
            doesn't confuse "we have no idea" with "measured zero".
        """
        from almanak.framework.teardown.models import PositionType

        # VIB-5313: principal-token (Pendle PT) positions are valued from the
        # gateway PT/USD price authority (MarketSnapshot.pt_price →
        # value_principal_token_position). Detected by DATA SHAPE — a ``pt_token``
        # in details — not by a protocol-name string (the VIB-4636 capability-gate
        # discipline that keeps this file free of connector-name coupling and the
        # framework→connector coupling ratchet green). Intercepted BEFORE the type
        # dispatch so a PT a strategy mis-reports under SUPPLY (e.g. exp8) does not
        # fall into the lending repricer and book its placeholder value. Returns
        # None for a position this path cannot value (no pt_token / no pt_price
        # surface) → fall through to the normal dispatch.
        details = getattr(position, "details", None) or {}
        if details.get("pt_token") or details.get("pt_token_symbol"):
            pt = self._reprice_principal_token_enriched(position, chain, market)
            if pt is not None:
                return pt

        if position.position_type == PositionType.LP:
            return self._reprice_lp_enriched_dispatch(position, chain, market)

        if position.position_type in (PositionType.SUPPLY, PositionType.BORROW):
            result = self._reprice_lending_on_chain_enriched(position, chain, market)
            if result is not None:
                return result[0], result[1], True
            # No on-chain path matched — fall back to the strategy-reported
            # value when it carries signal.
            #
            # BORROW debt is semantically negative; strategies may report
            # either the *gross* debt as positive (framework negates) or an
            # already-normalised negative value. Either is a real value;
            # only ``value_usd == 0`` means "no measurement".
            if position.position_type == PositionType.BORROW:
                if position.value_usd > 0:
                    return -position.value_usd, {}, True
                if position.value_usd < 0:
                    return position.value_usd, {}, True
                # value_usd == 0 — no signal → flag as no_path so confidence
                # drops to UNAVAILABLE rather than masquerade as measured zero.
                return position.value_usd, {}, False
            # SUPPLY — long-only; a positive value is the strategy's fallback
            # assertion, zero means we have nothing to say.
            if position.value_usd > 0:
                return position.value_usd, {}, True
            return position.value_usd, {}, False

        if position.position_type == PositionType.PERP:
            result = self._reprice_perps_on_chain_enriched(position, chain, market)
            if result is not None:
                return result[0], result[1], True
            # VIB-5252: the on-chain net-equity read did not match this perp.
            # In the common path Site A (``_merge_position_sources``) drops the
            # strategy's notional stub before it reaches here, so this is hit
            # only when discovery could NOT scan the venue (read failed / venue
            # undeclared) and the stub survived. A strategy perp's ``value_usd``
            # is gross NOTIONAL (collateral × leverage), NOT net equity — booking
            # it at ``repriced=True`` here would overstate NAV by leverage (the
            # original double-count, and the inert trap PR #2937 fell into).
            # Signal no_path instead: confidence drops to UNAVAILABLE (§7.5) so
            # the runner substitutes ``IntentStrategy.get_portfolio_snapshot``,
            # which excludes the perp notional rather than re-booking it (Site D).
            # Unlike the lending branches above we return ``0`` (not the reported
            # value): a lending stub's value is a real supply/debt amount, but a
            # perp stub's is inflated notional — if any UNAVAILABLE-tolerant
            # reader uses it, understating to 0 is safe; over-stating is not.
            return Decimal("0"), {}, False

        if position.position_type == PositionType.VAULT:
            result = self._reprice_vault_on_chain_enriched(position, chain, market)
            if result is not None:
                return result[0], result[1], True
            return position.value_usd, {}, True

        return position.value_usd, {}, True

    def _reprice_principal_token_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any], bool] | None:
        """Re-price a principal-token (PT) position via the gateway price authority (VIB-5313).

        The gateway is the single PT/USD price authority (design spine §0/§1): it
        composes ``PT/USD = pt_to_asset_rate × underlying/USD``, sources both
        legs, and stamps confidence + staleness. This path reads that one number
        via ``MarketSnapshot.pt_price`` (the strategy-safe surface — NO on-chain
        read, NO composition here) and multiplies by the wallet's PT holding
        through ``value_principal_token_position`` (the connector-skeleton math
        that owns only the position math, spine §1 boundary).

        Returns ``(value_usd, enriched_details, repriced)`` or ``None`` when the
        position is not a PT-shaped position this path can value (caller falls
        through to the type dispatch).

        Empty ≠ Zero (spine §3.3): an unmeasured gateway price (``UNAVAILABLE`` /
        ``price=None`` — the corrected VIB-5310 model returns no number when
        ``pt_to_asset_rate`` or the underlying is missing, never an at-par
        fabrication) returns ``repriced=False`` so the snapshot confidence drops
        to ``UNAVAILABLE`` — NEVER a booked ``Decimal("0")``. A degraded-but-
        measured price (``ESTIMATED`` / ``STALE``) is valued and flagged
        ``valuation_status="estimated"`` so the snapshot degrades to ESTIMATED
        (spine §3.4 — confidence never upgraded into a HIGH NAV).
        """
        from almanak.framework.portfolio.models import ValueConfidence

        details = getattr(position, "details", None) or {}
        # Symbol is the PT identity + price-contract key (spine §3.1).
        symbol = details.get("pt_token") or details.get("pt_token_symbol") or details.get("symbol")
        pt_price_fn = getattr(market, "pt_price", None)
        if not symbol or not callable(pt_price_fn):
            # No PT symbol, or the market surface predates pt_price (e.g. a bare
            # test double / data-layer snapshot) → not valuable here.
            return None

        # PT quantity = the wallet's PT holding (human units). PT is
        # KNOWN_UNPRICEABLE in the spot oracle, so it is dropped from the wallet
        # balance valuation — its USD value enters NAV ONLY here, no double-count.
        try:
            bal = market.balance(symbol)
            pt_amount = bal.balance if hasattr(bal, "balance") else Decimal(str(bal))
        except Exception as e:  # noqa: BLE001 — fail to unmeasured, never crash the snapshot
            logger.warning("principal-token reprice: balance(%s) failed (%s); cannot size PT position", symbol, e)
            # VIB-5317: stamp the PT-inventory marker (+ Empty≠Zero flag) so the
            # reported PT still surfaces on the displayed PT-inventory surface as
            # UNMEASURED — qty unknown here (balance failed), USD mark/PnL blank.
            return (
                Decimal("0"),
                {
                    "pt_symbol": symbol,
                    "source": _PT_INVENTORY_SOURCE,
                    "classification": "deployed_inventory",
                    "valuation_status": "no_path",
                    "mark_unmeasured": True,
                    "cost_basis_unmeasured": True,
                    "unrealized_pnl_unmeasured": True,
                },
                False,
            )

        try:
            pt_price = pt_price_fn(symbol, chain)
        except Exception as e:  # noqa: BLE001 — PriceUnavailableError (no gateway client) / transport
            logger.warning("principal-token reprice: pt_price(%s) failed (%s)", symbol, e)
            return (
                Decimal("0"),
                {
                    "pt_symbol": symbol,
                    "source": _PT_INVENTORY_SOURCE,
                    "classification": "deployed_inventory",
                    "quantity": str(pt_amount),
                    "valuation_status": "no_path",
                    "mark_unmeasured": True,
                    "cost_basis_unmeasured": True,
                    "unrealized_pnl_unmeasured": True,
                },
                False,
            )

        from almanak.connectors._strategy_base.principal_token_valuation import (
            value_principal_token_position,
        )

        valued = value_principal_token_position(pt_price=pt_price, pt_amount=pt_amount)

        # VIB-5317: stamp the SAME display fields the FIFO inventory path
        # (``_classify_pt_inventory``) uses so the dashboard / CLI render a
        # REPORTED PT (``details.pt_token``, the common ``get_open_positions``
        # case) identically to a FIFO-derived held PT. The ``source`` marker is
        # the VIB-4636 data-shape key every PT-inventory consumer detects on
        # (gateway ``_pt_strategy_positions_from_snapshot``, CLI status, dashboard
        # ``_extract_pt_inventory``) — never a protocol-name string. This is
        # display-only: ``value_usd`` / NAV / the FIFO dedup are untouched (the
        # dedup keys on ``pt_token`` / ``pt_symbol``, not ``source``).
        enriched: dict[str, Any] = {
            "pt_symbol": symbol,
            "source": _PT_INVENTORY_SOURCE,
            "classification": "deployed_inventory",
            "quantity": str(pt_amount),
            "pt_amount": str(pt_amount),
            "price_confidence": str(valued.confidence),
            "underlying_price_usd": (str(pt_price.underlying_price) if pt_price.underlying_price is not None else ""),
            "pt_to_asset_rate": (str(pt_price.pt_to_asset_rate) if pt_price.pt_to_asset_rate is not None else ""),
            "days_to_maturity": pt_price.days_to_maturity,
            "price_source": pt_price.source,
        }

        if valued.current_value_usd is None or valued.confidence == ValueConfidence.UNAVAILABLE:
            # Empty ≠ Zero: unmeasured price → no_path so the snapshot confidence
            # drops to UNAVAILABLE rather than masquerade as a measured $0. The
            # row still surfaces (qty shown) but with NO USD mark / cost / PnL.
            enriched["valuation_status"] = "no_path"
            enriched["mark_unmeasured"] = True
            enriched["cost_basis_unmeasured"] = True
            enriched["unrealized_pnl_unmeasured"] = True
            return Decimal("0"), enriched, False

        if valued.confidence != ValueConfidence.HIGH:
            # ESTIMATED / STALE gateway price → snapshot ESTIMATED (never folded
            # into a HIGH-confidence NAV). The value is real and traceable.
            enriched["valuation_status"] = "estimated"

        return valued.current_value_usd, enriched, True

    def _reprice_lp_on_chain_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any]] | None:
        """Re-price LP and return enriched details for snapshot persistence."""
        try:
            token_id = self._extract_token_id(position)
            if token_id is None:
                return None

            on_chain = self._lp_reader.read_position(chain=chain, token_id=token_id, protocol=position.protocol)
            if on_chain is None:
                return None

            if on_chain.liquidity == 0 and on_chain.tokens_owed0 == 0 and on_chain.tokens_owed1 == 0:
                return Decimal("0"), {"position_id": str(token_id), "liquidity": "0"}

            token0_symbol = self._resolve_token_symbol(on_chain.token0, position, "token0")
            token1_symbol = self._resolve_token_symbol(on_chain.token1, position, "token1")
            if not token0_symbol or not token1_symbol:
                return None

            try:
                token0_price = Decimal(str(market.price(token0_symbol)))
                token1_price = Decimal(str(market.price(token1_symbol)))
            except Exception:
                return None

            if token0_price <= 0 or token1_price <= 0:
                return None

            token0_decimals = self._get_token_decimals(token0_symbol, chain)
            token1_decimals = self._get_token_decimals(token1_symbol, chain)
            if token0_decimals is None or token1_decimals is None:
                return None

            # VIB-4274 — see ``_resolve_lp_pool_address_from_details`` for
            # the prefer-pool_address + hex-shape guard. Descriptor-shaped
            # values would slip into ``eth_call`` and trip
            # ``-32602 odd number of digits``; the price-ratio fallback
            # below masks the warning but the ``in_range`` flag would
            # silently lie on mainnet.
            pool_address = _resolve_lp_pool_address_from_details(position)
            current_tick: int | None = None
            sqrt_price_x96: int | None = None
            if pool_address:
                slot0 = self._lp_reader.read_pool_slot0(chain, pool_address)
                if slot0:
                    current_tick = slot0.tick
                    sqrt_price_x96 = slot0.sqrt_price_x96

            if current_tick is None:
                current_tick = self._price_ratio_to_tick(token0_price, token1_price, token0_decimals, token1_decimals)

            lp_value = value_lp_position(
                liquidity=on_chain.liquidity,
                tick_lower=on_chain.tick_lower,
                tick_upper=on_chain.tick_upper,
                current_tick=current_tick,
                token0_price_usd=token0_price,
                token1_price_usd=token1_price,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                sqrt_price_x96=sqrt_price_x96,
            )

            fees_usd = Decimal("0")
            fees0_human = Decimal("0")
            fees1_human = Decimal("0")
            if on_chain.tokens_owed0 > 0:
                fees0_human = Decimal(on_chain.tokens_owed0) / Decimal(10**token0_decimals)
                fees_usd += fees0_human * token0_price
            if on_chain.tokens_owed1 > 0:
                fees1_human = Decimal(on_chain.tokens_owed1) / Decimal(10**token1_decimals)
                fees_usd += fees1_human * token1_price

            total = lp_value.value_usd + fees_usd

            enriched = {
                "position_id": str(token_id),
                "amount0": str(lp_value.amount0),
                "amount1": str(lp_value.amount1),
                "token0_value_usd": str(lp_value.token0_value_usd),
                "token1_value_usd": str(lp_value.token1_value_usd),
                "in_range": lp_value.in_range,
                "tick_lower": on_chain.tick_lower,
                "tick_upper": on_chain.tick_upper,
                "liquidity": str(on_chain.liquidity),
                "fees0": str(fees0_human),
                "fees1": str(fees1_human),
                "fees_usd": str(fees_usd),
                "token0_symbol": token0_symbol,
                "token1_symbol": token1_symbol,
                "valuation_source": "on_chain",
            }

            return total, enriched

        except Exception:
            logger.debug("LP enriched re-pricing failed for %s", position.position_id, exc_info=True)
            return None

    def _reprice_v4_lp_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any], bool]:
        """Identity-faithful Uniswap V4 LP valuation (VIB-5018 / VIB-4586 / VIB-5024).

        V4 LP positions do NOT live on the V3 NonfungiblePositionManager, so the
        V3-shaped ``LPPositionReader.read_position`` cannot value them — routing a
        V4 tokenId there reads an unrelated NFT and corrupts both token identity
        and amount scaling (the $289M bug).

        Two valuation tiers, in confidence order:

        - **HIGH (VIB-5024)**: read the *live* position liquidity + tick range +
          pool slot0 on-chain through the gateway ``QueryV4PositionState`` RPC
          (boundary-compliant; addresses resolved connector-side), then compute
          exact amount0/amount1 via the shared concentrated-liquidity math
          (``value_lp_position`` — the same helper V3 uses). This reflects
          in-range drift, so it is true on-chain value.
        - **ESTIMATED (VIB-5018 fallback)**: when the live read is unavailable,
          value the position from the **receipt-parsed OPEN amounts** persisted on
          the Layer-3 ``position_events`` row re-marked at current prices.
          Identity comes from the OPEN event's token symbols, cross-checked
          against the canonical ``PoolKey`` resolved via the gateway
          (``LookupV4PoolKey``). Re-marking the opening amounts ignores subsequent
          drift / fee accrual, so a reader must not treat it as HIGH.

        Returns ``(value_usd, enriched_details, repriced)``. ``repriced`` is
        ``False`` only when neither tier can produce a value (no live read, no
        OPEN event, no identity, no price) — driving the snapshot to UNAVAILABLE
        (VIB-4584) rather than ever emitting a wrong value at HIGH confidence.
        """
        # Tier 1 — live on-chain read (HIGH). Never wrong-HIGH: any miss returns
        # None and falls through to the ESTIMATED OPEN-amount path below.
        live = self._reprice_v4_lp_live(position, chain, market)
        if live is not None:
            return live

        try:
            open_amounts = self._v4_open_amounts(position)
            if open_amounts is None:
                return self._v4_no_path(position)
            token0_symbol_open, token1_symbol_open, amount0_wei, amount1_wei = open_amounts

            # Identity: prefer the gateway PoolKey (authoritative addresses →
            # symbols); fall back to the OPEN event's reported symbols. Either
            # way this is identity-faithful — never the V3-read garbage.
            token0_symbol, token1_symbol = self._resolve_v4_symbols(
                position, chain, token0_symbol_open, token1_symbol_open
            )
            if not token0_symbol or not token1_symbol:
                return self._v4_no_path(position)

            try:
                token0_price = Decimal(str(market.price(token0_symbol)))
                token1_price = Decimal(str(market.price(token1_symbol)))
            except Exception:
                return self._v4_no_path(position)
            if token0_price <= 0 or token1_price <= 0:
                return self._v4_no_path(position)

            token0_decimals = self._get_token_decimals(token0_symbol, chain)
            token1_decimals = self._get_token_decimals(token1_symbol, chain)
            if token0_decimals is None or token1_decimals is None:
                return self._v4_no_path(position)

            amount0 = Decimal(amount0_wei) / Decimal(10**token0_decimals)
            amount1 = Decimal(amount1_wei) / Decimal(10**token1_decimals)
            token0_value_usd = amount0 * token0_price
            token1_value_usd = amount1 * token1_price
            total = token0_value_usd + token1_value_usd

            enriched = {
                "position_id": str(position.position_id or ""),
                "amount0": str(amount0),
                "amount1": str(amount1),
                "token0_value_usd": str(token0_value_usd),
                "token1_value_usd": str(token1_value_usd),
                "token0_symbol": token0_symbol,
                "token1_symbol": token1_symbol,
                "valuation_source": "v4_open_amounts",
                "valuation_status": "estimated",
            }
            return total, enriched, True

        except Exception:
            logger.debug("V4 LP enriched re-pricing failed for %s", position.position_id, exc_info=True)
            return self._v4_no_path(position)

    def _reprice_v4_lp_live(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any], bool] | None:
        """Tier-1 HIGH-confidence V4 LP valuation from a live on-chain read (VIB-5024).

        Reads live liquidity + tick range + pool slot0 via the gateway
        ``QueryV4PositionState`` RPC (boundary-compliant; addresses connector-
        resolved), resolves identity-faithful token symbols (same path the
        ESTIMATED tier uses — canonical PoolKey first, OPEN-event symbols as the
        fallback pair), and computes exact amount0/amount1 with the shared
        concentrated-liquidity math ``value_lp_position``.

        Returns ``(value_usd, enriched_details, True)`` with
        ``valuation_status="onchain"`` on success, or ``None`` when ANY input is
        missing / unmeasured (no gateway, no live read, no identity, no price, no
        decimals). ``None`` makes the caller fall back to the ESTIMATED OPEN-amount
        path — never a wrong HIGH (VIB-4584 / never-wrong-HIGH guarantee).
        """
        try:
            state = self._v4_live_state(position, chain)
            if state is None:
                return None

            # Identity integrity (never-wrong-HIGH): the gateway returns the
            # AUTHORITATIVE pool_id — keccak of the *tokenId's* on-chain PoolKey —
            # while symbols below are resolved from the position's STORED pool_id.
            # If a corrupt stored pool_id disagrees with the tokenId's real pool,
            # live on-chain amounts would pair with wrong-pool symbols at HIGH
            # confidence (the $289M identity-bug class). When both are present and
            # diverge, fall back to ESTIMATED rather than value at HIGH.
            stored_pool_id = self._extract_v4_pool_id(position)
            state_pool_id = (getattr(state, "pool_id", "") or "").lower().removeprefix("0x")
            if stored_pool_id and state_pool_id and stored_pool_id != state_pool_id:
                logger.warning(
                    "V4 live re-pricing: stored pool_id 0x%s != on-chain pool_id 0x%s for "
                    "position %s; falling back to ESTIMATED (never wrong-HIGH)",
                    stored_pool_id,
                    state_pool_id,
                    position.position_id,
                )
                return None

            # Identity: prefer the canonical PoolKey, fall back to the OPEN
            # event's (already canonically-sorted) symbol pair. The receipt parser
            # emits the OPEN amounts in canonical currency0<currency1 order, the
            # same order the live on-chain amounts use — so reusing the OPEN-event
            # symbols as the identity source keeps amount↔symbol alignment intact.
            # We reuse the OPEN-event symbols only as the identity source — the
            # AMOUNTS here are the live on-chain amounts, not the opening amounts.
            open_amounts = self._v4_open_amounts(position)
            token0_open = open_amounts[0] if open_amounts else ""
            token1_open = open_amounts[1] if open_amounts else ""
            token0_symbol, token1_symbol = self._resolve_v4_symbols(position, chain, token0_open, token1_open)
            if not token0_symbol or not token1_symbol:
                return None

            try:
                token0_price = Decimal(str(market.price(token0_symbol)))
                token1_price = Decimal(str(market.price(token1_symbol)))
            except Exception:
                return None
            if token0_price <= 0 or token1_price <= 0:
                return None

            token0_decimals = self._get_token_decimals(token0_symbol, chain)
            token1_decimals = self._get_token_decimals(token1_symbol, chain)
            if token0_decimals is None or token1_decimals is None:
                return None

            lp_value = value_lp_position(
                liquidity=state.liquidity,
                tick_lower=state.tick_lower,
                tick_upper=state.tick_upper,
                current_tick=state.current_tick,
                token0_price_usd=token0_price,
                token1_price_usd=token1_price,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                sqrt_price_x96=state.sqrt_price_x96,
            )

            # V3 parity (VIB-5024): a HIGH valuation includes uncollected fees.
            # ``state`` duck-types as ``LPPositionOnChain`` (tokens_owed0/1); the
            # gateway fails closed if it cannot measure them, so on a live read
            # they are present (measured 0 stays 0 — Empty≠Zero).
            fees_usd = self._compute_lp_uncollected_fees_usd(
                state,
                token0_price,
                token1_price,
                token0_decimals,
                token1_decimals,
            )
            total_value_usd = lp_value.value_usd + fees_usd

            enriched = {
                "position_id": str(position.position_id or ""),
                "amount0": str(lp_value.amount0),
                "amount1": str(lp_value.amount1),
                "token0_value_usd": str(lp_value.token0_value_usd),
                "token1_value_usd": str(lp_value.token1_value_usd),
                "tokens_owed0": str(state.tokens_owed0),
                "tokens_owed1": str(state.tokens_owed1),
                "fees_usd": str(fees_usd),
                "in_range": lp_value.in_range,
                "tick_lower": state.tick_lower,
                "tick_upper": state.tick_upper,
                "liquidity": str(state.liquidity),
                "token0_symbol": token0_symbol,
                "token1_symbol": token1_symbol,
                "valuation_source": "v4_on_chain",
                "valuation_status": "onchain",
            }
            return total_value_usd, enriched, True
        except Exception:
            logger.debug("V4 live LP re-pricing failed for %s", position.position_id, exc_info=True)
            return None

    def _v4_live_state(self, position: "PositionInfo", chain: str) -> Any | None:
        """Read live V4 position state via the connector-backed gateway reader (VIB-5024).

        Boundary-compliant AND connector-self-contained: routes through the
        protocol-agnostic ``STRATEGY_RUNNER_HOOK_REGISTRY.build_v4_position_state_reader``
        capability seam, which the V4 connector backs with the gateway
        ``QueryV4PositionState`` RPC (PositionManager + StateView addresses
        resolved connector-side). Returns ``None`` on any miss (no gateway, no
        token_id, no reader capability, gateway failure) so the caller falls back
        to the ESTIMATED OPEN-amount path.
        """
        if self._gateway_client is None:
            return None
        token_id = self._extract_token_id(position)
        if token_id is None:
            return None
        try:
            from almanak.connectors._strategy_runner_hook_registry import (
                STRATEGY_RUNNER_HOOK_REGISTRY,
            )

            reader = STRATEGY_RUNNER_HOOK_REGISTRY.build_v4_position_state_reader(self._gateway_client)
            if reader is None:
                return None
            return reader(chain, token_id)
        except Exception:
            logger.debug(
                "V4 live position-state read failed for position=%s token_id=%s",
                position.position_id,
                token_id,
                exc_info=True,
            )
            return None

    def _v4_open_amounts(self, position: "PositionInfo") -> tuple[str, str, int, int] | None:
        """Read the OPEN ``position_events`` row for a V4 LP position.

        Returns ``(token0_symbol, token1_symbol, amount0_wei, amount1_wei)`` from
        the receipt-parsed open, or ``None`` when no usable OPEN row exists
        (no accounting store / no row / unparseable amounts). The OPEN amounts are
        the authoritative, identity-faithful token amounts the connector emitted
        from the LP_OPEN receipt — the framework's only boundary-compliant source
        of V4 LP token amounts until a gateway V4 position reader exists.

        VIB-5018 (live re-baseline) — the same-iteration runner cache
        (``_recent_open_events``, VIB-3894) is checked first for speed, but its
        dict does NOT carry ``amount0`` / ``amount1`` for every primitive (the
        runner stamps them only when the OPEN event surfaces them). A cache hit
        that lacks usable amounts MUST fall through to the store query — which
        always carries them — instead of short-circuiting to no_path. Treating
        "cache present but incomplete" as a cache miss is what makes the
        ESTIMATED path actually fire in the live snapshot pipeline.
        """
        position_id = position.position_id
        if not position_id:
            return None

        # Prefer the in-memory runner cache, but accept it ONLY when complete.
        cache = getattr(self, "_recent_open_events", None) or {}
        parsed = self._parse_v4_open_event(cache.get((str(position_id), "LP")))
        if parsed is not None:
            return parsed

        # Cache miss / cache incomplete → authoritative store query (carries amounts).
        # Guard the deployment scope explicitly (mirrors the position_id guard
        # above): an unconfigured valuer with ``_deployment_id == ""`` must not
        # issue a deployment-wide store query (pr-audit #4 defense-in-depth).
        if (
            self._accounting_store is None
            or not self._deployment_id
            or not hasattr(self._accounting_store, "get_position_events_sync")
        ):
            return None
        try:
            events = self._accounting_store.get_position_events_sync(
                self._deployment_id,
                position_id=position_id,
                position_type="LP",
                event_type="OPEN",
            )
        except Exception:
            return None
        if not events:
            return None
        return self._parse_v4_open_event(events[0])

    @staticmethod
    def _parse_v4_open_event(open_event: object) -> tuple[str, str, int, int] | None:
        """Parse one OPEN ``position_events`` dict into the V4 valuation tuple.

        Returns ``(token0_symbol, token1_symbol, amount0_wei, amount1_wei)`` only
        when ALL four are present and well-formed; ``None`` otherwise (so an
        incomplete runner-cache dict reads as a miss rather than a measured zero).
        Empty ≠ Zero: an absent / ``""`` / unparseable amount is ``None`` (miss),
        not a measured zero.
        """
        if not isinstance(open_event, dict):
            return None
        token0_symbol = open_event.get("token0")
        token1_symbol = open_event.get("token1")
        amount0_wei = PortfolioValuer._coerce_int(open_event.get("amount0"))
        amount1_wei = PortfolioValuer._coerce_int(open_event.get("amount1"))
        if (
            not isinstance(token0_symbol, str)
            or not token0_symbol
            or not isinstance(token1_symbol, str)
            or not token1_symbol
            or amount0_wei is None
            or amount1_wei is None
        ):
            return None
        return token0_symbol, token1_symbol, amount0_wei, amount1_wei

    def _resolve_v4_symbols(
        self,
        position: "PositionInfo",
        chain: str,
        token0_symbol_open: str,
        token1_symbol_open: str,
    ) -> tuple[str | None, str | None]:
        """Resolve identity-faithful (token0, token1) symbols for a V4 position.

        Prefers the canonical ``PoolKey`` resolved from the V4 ``pool_id`` via the
        gateway (authoritative on-chain addresses → symbols), falling back to the
        OPEN event's reported symbols. The OPEN symbols are themselves derived
        from the receipt, so both sources are identity-faithful — neither is the
        V3-read corruption.
        """
        pool_key = self._resolve_v4_pool_key(position, chain)
        if pool_key is not None:
            # Resolve BOTH currencies from their on-chain addresses ONLY. We do
            # NOT use ``_resolve_token_symbol`` here: its strategy-metadata
            # fallback reads ``details["token0"/"token1"]`` (user order), which
            # can splice a user-order symbol into a sorted ``currency0<currency1``
            # slot when only one currency resolves — a silent identity mix
            # (pr-audit Important #1). Either both addresses resolve as a sorted
            # pair, or we fall back to the OPEN-event pair below — the receipt
            # parser already emits those in canonical currency0<currency1 order,
            # so they stay paired with the sorted amount0/amount1.
            sym0 = self._symbol_from_address(pool_key.currency0, chain)
            sym1 = self._symbol_from_address(pool_key.currency1, chain)
            if sym0 and sym1:
                return sym0, sym1
        return token0_symbol_open or None, token1_symbol_open or None

    @staticmethod
    def _symbol_from_address(token_address: str, chain: str) -> str | None:
        """Resolve a token symbol from its on-chain address ONLY.

        Unlike ``_resolve_token_symbol`` there is NO strategy-metadata fallback:
        a miss returns ``None`` so :meth:`_resolve_v4_symbols` falls back to the
        (already canonically-sorted) OPEN-event symbol *pair* rather than
        splicing a user-order ``details`` symbol into a sorted PoolKey slot.
        """
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            resolved = resolver.resolve(token_address, chain)
            if resolved and resolved.symbol:
                return resolved.symbol
        except Exception:
            return None
        return None

    @staticmethod
    def _v4_no_path(position: "PositionInfo") -> tuple[Decimal, dict[str, Any], bool]:
        """V4 path could not value this position.

        VIB-5018 / VIB-4584 — never emit a wrong value at HIGH. When the V4 path
        cannot produce an identity-faithful value, fall back ONLY to a positive
        strategy-reported value (an explicit assertion we trust); otherwise flag
        ``no_path`` so the snapshot confidence drops to UNAVAILABLE rather than
        masquerading as a measured zero.
        """
        if position.value_usd > 0:
            return position.value_usd, {"valuation_status": "estimated"}, True
        return position.value_usd, {}, False

    def _resolve_v4_pool_key(self, position: "PositionInfo", chain: str) -> Any | None:
        """Resolve the canonical V4 PoolKey from the position's pool_id via the gateway.

        Boundary-compliant AND connector-self-contained: routes through the
        protocol-agnostic ``STRATEGY_RUNNER_HOOK_REGISTRY.build_pool_key_lookup``
        capability seam (Blueprint 22), which the V4 connector backs with the
        gateway ``MarketService.LookupV4PoolKey`` RPC — no direct chain RPC and no
        framework→connector import. Returns ``None`` on any failure (no pool_id,
        gateway unavailable, no lookup capability registered, NOT_FOUND, unexpected
        error) so the caller falls back to the receipt-reported identity.
        """
        if self._gateway_client is None:
            return None
        pool_id = self._extract_v4_pool_id(position)
        if not pool_id:
            return None
        try:
            from almanak.connectors._strategy_runner_hook_registry import (
                STRATEGY_RUNNER_HOOK_REGISTRY,
            )

            lookup = STRATEGY_RUNNER_HOOK_REGISTRY.build_pool_key_lookup(self._gateway_client)
            if lookup is None:
                return None
            return lookup(pool_id, chain)
        except Exception:
            logger.debug(
                "V4 PoolKey lookup failed for position=%s pool_id=%s",
                position.position_id,
                pool_id,
                exc_info=True,
            )
            return None

    @classmethod
    def _is_v4_lp_position(cls, position: "PositionInfo") -> bool:
        """Detect a Uniswap-V4-stream LP position by DATA SHAPE, not protocol name.

        VIB-4636 capability-gate discipline: a V4 LP carries a 64-hex PoolKey hash
        in ``details`` (``pool_id`` / legacy ``pool_address`` / ``pool``); a V3 LP
        carries a 40-hex pool *contract* address (or none), since a V4 pool has no
        contract address (it lives in the singleton PoolManager). This keeps the
        framework valuer free of any hardcoded ``"uniswap_v4"`` protocol string.
        """
        return cls._extract_v4_pool_id(position) is not None

    @staticmethod
    def _extract_v4_pool_id(position: "PositionInfo") -> str | None:
        """Extract the 32-byte V4 pool_id (64-hex string) from position details.

        The V4 connector stashes the pool_id under ``pool_id`` or (legacy) in the
        ``pool_address`` slot — a V4 pool has no contract address, so a 64-hex
        ``pool_address`` is actually the pool_id. A 40-hex (EVM-address-shaped)
        value is NOT a pool_id and is rejected.
        """
        for key in ("pool_id", "pool_address", "pool"):
            raw = position.details.get(key)
            if not isinstance(raw, str) or not raw:
                continue
            clean = raw.lower().removeprefix("0x")
            if len(clean) == _V4_POOL_ID_HEX_LEN and all(c in "0123456789abcdef" for c in clean):
                return clean
        return None

    @staticmethod
    def _coerce_int(value: object) -> int | None:
        """Coerce a stored liquidity / tick / amount (wei) value to int, or None if unparseable.

        Empty ≠ Zero: ``None`` / ``""`` / unparseable → ``None`` (unmeasured);
        a real numeric string or int → its int value (``"0"`` → measured zero).
        """
        if value is None or value == "":
            return None
        if not isinstance(value, int | str):
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _reprice_lending_on_chain_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any]] | None:
        """Re-price lending position and return enriched details."""
        from almanak.framework.teardown.models import PositionType

        try:
            asset_address = self._extract_asset_address(position)
            if not asset_address:
                asset_symbol = position.details.get("asset")
                if asset_symbol:
                    try:
                        from almanak.framework.data.tokens import get_token_resolver

                        resolved = get_token_resolver().resolve(asset_symbol, chain)
                        if resolved and resolved.address:
                            asset_address = resolved.address
                    except Exception:
                        pass

            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            if not asset_address or not wallet_address:
                return None

            on_chain = self._lending_reader.read_position(
                chain=chain,
                asset_address=asset_address,
                wallet_address=wallet_address,
                protocol=position.protocol,
            )
            if on_chain is None:
                return None

            if not on_chain.is_active:
                return Decimal("0"), {"valuation_source": "on_chain", "is_active": False}

            token_symbol = self._resolve_token_symbol(on_chain.asset_address, position, "asset")
            if not token_symbol:
                token_symbol = position.details.get("asset")
            if not token_symbol:
                return None

            try:
                token_price = Decimal(str(market.price(token_symbol)))
            except Exception:
                return None

            if token_price <= 0:
                return None

            token_decimals = self._get_token_decimals(token_symbol, chain)
            if token_decimals is None:
                return None

            valued = value_lending_position(
                atoken_balance=on_chain.current_atoken_balance,
                stable_debt=on_chain.current_stable_debt,
                variable_debt=on_chain.current_variable_debt,
                token_price_usd=token_price,
                token_decimals=token_decimals,
                collateral_enabled=on_chain.usage_as_collateral_enabled,
                asset=token_symbol,
            )

            # USD legs are MeasuredMoney (VIB-5216). The price is guarded measured
            # above, so ``.value_or`` returns the real value here; an unmeasured
            # price would yield Decimal("0") rather than a wrong sign — but it
            # cannot occur on this path (guarded), keeping the wire bytes identical.
            if position.position_type == PositionType.BORROW:
                result_value = (-valued.debt_value_usd).value_or(Decimal("0"))
            else:
                result_value = valued.net_value_usd.value_or(Decimal("0"))

            # Serialize the USD legs through the MeasuredMoney payload codec
            # (measured→str, unmeasured→None) so persistence stays byte-compatible
            # and a non-measured value is never fabricated as "0".
            enriched = {
                "supply_balance": str(valued.supply_balance),
                "supply_value_usd": valued.supply_value_usd.to_payload(),
                "stable_debt_balance": str(valued.stable_debt_balance),
                "variable_debt_balance": str(valued.variable_debt_balance),
                "debt_value_usd": valued.debt_value_usd.to_payload(),
                "net_value_usd": valued.net_value_usd.to_payload(),
                "collateral_enabled": valued.collateral_enabled,
                "valuation_source": "on_chain",
            }

            # VIB-5006: stamp the Track-C lending observability fields the
            # ``position_state_snapshots`` materialiser
            # (``_materialise_lending``) reads but that were never populated —
            # the Accountant L5 (APR/APY) gap and part of the L2/L3 (HF) gap.
            #
            # ``borrow_balance``: this reserve's total debt in HUMAN units
            #   (stable + variable). A measured ``Decimal("0")`` on a
            #   supply-only reserve — NOT ``None`` (Empty ≠ Zero); both legs
            #   reach here only after a successful on-chain read.
            # ``supply_apy_pct``: the reserve's supply rate. Aave-fork
            #   ``getUserReserveData`` returns ``liquidityRate`` in ray (1e27);
            #   render as a percentage. (The variable BORROW rate is NOT in
            #   ``getUserReserveData`` — it is a reserve-level read the connector
            #   does not yet expose — so ``borrow_apy_pct`` stays unmeasured
            #   here; tracked for the borrow-rate read follow-up. L5 passes on
            #   ``supply_apy_pct`` OR ``borrow_apy_pct``, so the supply leg
            #   already satisfies it.)
            # ``health_factor`` is account-level (``getUserAccountData``), not a
            #   per-reserve field — it is enriched once per (protocol, chain,
            #   wallet) in ``_get_positions`` via the account-state reader, so it
            #   is deliberately NOT set here (the old ``hasattr`` line was always
            #   ``None`` because ``LendingPositionOnChain`` has no HF field).
            enriched["borrow_balance"] = str(valued.stable_debt_balance + valued.variable_debt_balance)
            # Stamp unconditionally past the ``on_chain is None`` guard: the rate
            # is measured, so a genuine 0 ray ⇒ "0" (Empty ≠ Zero), never absent.
            # NOTE: this is the *reserve's* supply rate (``liquidityRate``) — so a
            # BORROW leg carries the supply APY of the borrowed reserve, NOT a
            # borrow APR. The borrow rate is a separate reserve-level read the
            # connector does not expose yet (``borrow_apy_pct`` stays unmeasured).
            enriched["supply_apy_pct"] = str((Decimal(on_chain.liquidity_rate) / _RAY_SCALE) * Decimal("100"))

            return result_value, enriched

        except Exception:
            logger.debug("Lending enriched re-pricing failed for %s", position.position_id, exc_info=True)
            return None

    def _enrich_lending_trackc_fields(
        self,
        position: "PositionInfo",
        chain: str,
        enriched_details: dict[str, Any],
        account_state_cache: dict[tuple[str, str, str, str | None], Any],
        market: MarketDataSource,
        *,
        strategy_wallet: str | None = None,
    ) -> dict[str, Any]:
        """Stamp Track-C lending observability fields via one capability-dispatched seam.

        The Track-C ``position_state_snapshots`` materialiser
        (``accounting/position_state.py:_materialise_lending``) reads
        ``health_factor`` / ``supply_apy_pct`` / ``borrow_apy_pct`` /
        ``borrow_balance`` straight off ``PositionValue.details``. This method
        populates them for any registered lending connector, dispatching on
        **connector capability** (never a protocol-name ``if/elif``), mirroring
        :meth:`PositionHealthProvider.get_health`:

        * **health_factor** — protocols publishing a multi-collateral
          market-health reader (Compound V3, VIB-5160) read via
          :func:`read_lending_market_health` (the summed
          ``Σ(value×LCF)/debt`` HF the single-leg account-state read cannot
          express); whole-account protocols (the Aave family, VIB-5006 — no
          per-market id) read via :func:`read_lending_account_state`. Both route
          through the gateway-boundary-correct lending-read seams (no
          framework-side RPC) and are cached per ``(protocol, chain, wallet,
          market_id)`` per snapshot — a leverage loop's SUPPLY + BORROW legs
          share one read. The *raw* reducer HF is stamped (a no-debt position's
          sentinel stays a number, not normalised to Infinity, so the cell
          scorers parse it — and so Compound matches the Aave convention).
          Per-market protocols whose account-state read declares priceable
          valuation roles (Morpho Blue — VIB-4551, plus Silo V2 / Euler V2 / Fluid,
          which ride the same capability-gated path) read the aggregate account
          state with the market's collateral/loan token prices injected
          (non-USD-native), scoped by ``market_id``. A per-market protocol needing
          a different injection (BENQI's collaterals-map) is excluded — it would
          fail closed and only appear wired.
        * **supply_apy_pct / borrow_apy_pct** — for market-health protocols
          (Compound) via the gateway-routed :meth:`MarketSnapshot.lending_rate`.
          The Aave family already carries ``supply_apy_pct`` from
          :meth:`_reprice_lending_on_chain_enriched` (its single-reserve
          repricer), so this method is idempotent — it fills only keys that path
          left absent, keeping the Aave leg byte-identical. Morpho's live rate is
          unmeasured until VIB-5040 (its gateway provider raises), so its APY
          stays ``None`` honestly.

        Empty ≠ Zero, two ways: a real read stamps the measured value; an
        *attempted but failed/None* read stamps an explicit ``None`` so a stale
        strategy-reported value cannot survive the downstream merge and
        masquerade as live (the VIB-5084 stale class). For positions we do NOT
        own (non-lending, no gateway, no wallet) the details are returned
        untouched.
        """
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
        from almanak.framework.teardown.models import PositionType

        if position.position_type not in (PositionType.SUPPLY, PositionType.BORROW):
            return enriched_details
        if self._gateway_client is None:
            return enriched_details

        protocol = position.protocol
        has_market_health = LendingReadRegistry.market_health_reader(protocol) is not None
        # Per-market lending protocols we can value here: Compound V3 (summed
        # market-health reader) and protocols whose account-state read declares
        # priceable valuation roles (Morpho Blue, Silo V2, Euler V2, Fluid —
        # non-USD-native, valued via injected token prices). Gate on
        # ``declares_valuation_roles``, NOT ``publishes_market_table``: a
        # per-market protocol that needs a *different* injection (BENQI's
        # collaterals-map, ``valuation_role_keys=()``) is deliberately excluded —
        # routing it through the priced read would fail closed forever, looking
        # wired but inert. The Aave family declares no roles ⇒ whole-account.
        is_per_market = has_market_health or LendingReadRegistry.declares_valuation_roles(protocol)

        # Resolve the market id for per-market protocols, accepting both detail
        # spellings — on-chain discovery uses ``market_id`` while a strategy's
        # ``get_open_positions`` reports ``market``. Whole-account protocols (the
        # Aave family) carry no market id; force ``None`` so a stray detail key can
        # never mis-route a whole-account leg into the per-market read path
        # (keeps the Aave path byte-neutral).
        if is_per_market:
            market_id = position.details.get("market_id") or position.details.get("market")
            if not market_id:
                # A per-market lending leg we recognise but cannot scope (no market
                # id) — fail closed: this seam owns the leg's Track-C fields, so
                # stamp explicit None for HF AND APY so a stale strategy-reported
                # value cannot survive the downstream merge (VIB-5084 class),
                # rather than returning the details unchanged. (Aave is never
                # per-market, so this never touches its repricer-set APY.)
                return {
                    **enriched_details,
                    "health_factor": None,
                    "supply_apy_pct": None,
                    "borrow_apy_pct": None,
                }
        else:
            market_id = None

        wallet = (
            position.details.get("wallet") or position.details.get("wallet_address") or position.details.get("owner")
        )
        # Per-market lending protocols (Compound V3, Morpho Blue) have no
        # single-reserve discovery spec, so their legs arrive only via the
        # strategy's ``get_open_positions`` — which reports the asset + market but
        # not the owner. Fall back to the deployment wallet (1 gateway : 1 strategy
        # ⇒ one owner) so the read can execute. The Aave family is discovered with
        # its wallet on-chain, so this fallback never changes its byte-neutral path.
        if not wallet and is_per_market:
            wallet = strategy_wallet
        if not wallet:
            return enriched_details
        # EVM addresses are case-insensitive — normalise so a checksummed and a
        # lowercase spelling of the same wallet (e.g. strategy-reported vs
        # on-chain-discovered) share ONE cached account-state read instead of
        # missing the cache and issuing a redundant gateway round-trip (Gemini).
        wallet = wallet.lower()

        key = (protocol, chain, wallet, market_id)
        if key not in account_state_cache:
            account_state_cache[key] = self._read_lending_trackc_state(
                protocol=protocol,
                chain=chain,
                wallet=wallet,
                market_id=market_id,
                has_market_health=has_market_health,
                market=market,
            )
        state = account_state_cache[key]

        # We attempted a supported read (past every early-return), so this leg's
        # HF is OURS to set: the measured value, or an explicit None that
        # overrides any stale strategy-reported HF in the merge below
        # (honest-unmeasured, not stale-passthrough).
        measured_hf = state.health_factor if state is not None else None
        out = {**enriched_details, "health_factor": str(measured_hf) if measured_hf is not None else None}

        # APY for multi-collateral protocols (Compound): the Aave family already
        # stamps supply_apy_pct via its single-reserve repricer, so fill here only
        # for protocols that path does not cover. Idempotent — never clobber a
        # value the repricer already set (Aave stays byte-identical).
        if has_market_health and market_id:
            # The lending-rate provider keys on the Comet's *base token symbol*
            # (e.g. "USDC"), NOT the lowercase market key ("usdc") — and the
            # mapping is case-sensitive, so a naive ``market_id.upper()`` breaks
            # for cased symbols (wstETH, USDC.e). Resolve the canonical base symbol
            # from the connector's market table (the same inputs the health read
            # uses); fall back to the leg's reported asset.
            inputs = LendingReadRegistry.market_health_inputs(protocol, chain, market_id)
            base_symbol = (inputs or {}).get("base_token") or position.details.get("asset")
            if base_symbol:
                base_symbol = str(base_symbol)
                if out.get("supply_apy_pct") is None:
                    out["supply_apy_pct"] = self._lending_rate_pct(market, protocol, base_symbol, "supply", chain)
                if out.get("borrow_apy_pct") is None:
                    out["borrow_apy_pct"] = self._lending_rate_pct(market, protocol, base_symbol, "borrow", chain)
        elif is_per_market:
            # Per-market protocols without a market-health rate source (Morpho
            # Blue, role-based) — the seam owns these fields but has no live-rate
            # source yet (Morpho live rate is VIB-5040), so stamp an explicit None
            # rather than leaving the keys absent: a stale strategy-reported APY
            # must not survive the merge. (Aave is whole-account, never per-market,
            # so its repricer-set supply_apy_pct is untouched — byte-neutral.)
            out["supply_apy_pct"] = None
            out["borrow_apy_pct"] = None

        return out

    def _read_lending_trackc_state(
        self,
        *,
        protocol: str,
        chain: str,
        wallet: str,
        market_id: str | None,
        has_market_health: bool,
        market: MarketDataSource,
    ) -> Any:
        """Read the aggregate lending state a leg's Track-C HF derives from.

        Routes through the capability-appropriate gateway-boundary-correct seam:
        the summed multi-collateral :func:`read_lending_market_health` for
        market-health protocols (Compound V3 — the base/borrow token price +
        decimals are resolved by a :class:`PositionHealthProvider` and threaded
        in; the per-collateral price/scale/liquidation factor are read on-chain
        by the connector reader), else the aggregate
        :func:`read_lending_account_state` for whole-account protocols (the Aave
        family — USD-native, no price oracle needed). Track-C is an observational
        snapshot so ``block=None`` ("latest") is correct (not a pinned
        post-state read). Fails closed to ``None`` (Empty ≠ Zero) on any error.
        """
        try:
            if has_market_health:
                if not market_id:
                    return None
                from almanak.framework.accounting.lending_reads import read_lending_market_health
                from almanak.framework.data.position_health import PositionHealthProvider

                # Reuse the canonical base-token price/decimals resolvers (USD
                # stablecoin 1:1 fallback + never-guess-decimals safety). Only the
                # resolvers are used here — they read ``price_oracle`` + the token
                # resolver, NOT the gateway client — so we pass ``gateway_client=None``
                # (the market-health read itself routes through ``self._gateway_client``
                # in ``read_lending_market_health`` below).
                php = PositionHealthProvider(
                    chain=chain,
                    gateway_client=None,
                    price_oracle=(lambda s: market.price(s)) if market is not None else None,
                )
                return read_lending_market_health(
                    protocol=protocol,
                    chain=chain,
                    wallet_address=wallet,
                    market_id=market_id,
                    gateway_client=self._gateway_client,
                    resolve_base_price=php._resolve_base_price,
                    resolve_base_decimals=php._resolve_base_decimals,
                )

            from almanak.framework.accounting.lending_reads import read_lending_account_state

            if market_id:
                # Per-market, non-USD-native protocol (Morpho Blue, VIB-4551): price
                # the market's collateral + loan tokens via the gateway-routed market
                # and inject them so the account-state reducer can value the position
                # and derive HF. A missing price ⇒ the read fails closed (None).
                price_oracle = self._build_lending_price_oracle(protocol, chain, market_id, market)
                return read_lending_account_state(
                    protocol=protocol,
                    chain=chain,
                    wallet_address=wallet,
                    market_id=market_id,
                    gateway_client=self._gateway_client,
                    price_oracle=price_oracle,
                    block=None,
                )

            return read_lending_account_state(
                protocol=protocol,
                chain=chain,
                wallet_address=wallet,
                market_id=None,
                gateway_client=self._gateway_client,
                price_oracle=None,
                block=None,
            )
        except Exception:
            logger.debug("Track-C lending state read failed for %s on %s", protocol, chain, exc_info=True)
            return None

    def _build_lending_price_oracle(
        self,
        protocol: str,
        chain: str,
        market_id: str,
        market: MarketDataSource,
    ) -> dict[str, Any] | None:
        """USD price map for a per-market lending read's valuation roles.

        Non-USD-native protocols (Morpho Blue) declare which tokens the
        account-state reducer must value (``valuation_roles`` → the market's
        collateral + loan symbols); this resolves each to a USD price via the
        gateway-routed :meth:`MarketSnapshot.price`. Empty ≠ Zero: a token whose
        price is unavailable is omitted, so the downstream read fails closed
        (returns ``None``) rather than valuing an incomplete set. Returns ``None``
        when there are no roles (the read then needs no oracle) or none priced.
        """
        if market is None:
            return None
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        try:
            roles = LendingReadRegistry.valuation_roles(protocol, chain, market_id)
        except Exception:
            return None
        oracle: dict[str, Any] = {}
        for _query_field, symbol in roles:
            try:
                price = market.price(symbol)
            except Exception:
                continue
            if price is None:
                continue
            try:
                oracle[symbol] = Decimal(str(price))
            except (InvalidOperation, TypeError, ValueError):
                continue
        return oracle or None

    def _lending_rate_pct(
        self,
        market: MarketDataSource,
        protocol: str,
        token: str,
        side: str,
        chain: str,
    ) -> str | None:
        """Gateway-routed live lending APY as a stringified percent, or ``None``.

        Empty ≠ Zero: an unavailable rate (no market, protocol raises
        ``RateHistoryUnavailable``, reduced ``market`` lacking the accessor)
        stamps an explicit ``None``, never a fabricated rate. Never raises into
        the snapshot path.
        """
        # ``lending_rate`` lives on the concrete MarketSnapshot, not the narrow
        # ``MarketDataSource`` protocol — resolve it dynamically so a reduced
        # market source simply yields ``None`` (Empty ≠ Zero) instead of raising.
        rate_fn = getattr(market, "lending_rate", None)
        if not callable(rate_fn):
            return None
        try:
            rate = rate_fn(protocol, token, side, chain=chain)
        except Exception:
            return None
        pct = getattr(rate, "apy_percent", None)
        return str(pct) if pct is not None else None

    def _value_matched_perp(  # noqa: C901
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> "PerpsPositionValue | None":
        """Resolve, match, and mark-to-market a perp position via the registry.

        The single shared body behind ``_reprice_perps_on_chain`` (returns the
        net value) and ``_reprice_perps_on_chain_enriched`` (also returns the
        per-position breakdown dict). Both wrappers consume the returned
        :class:`PerpsPositionValue` — net value is ``valued.net_value_usd``, the
        enriched dict is built from ``valued.*``.

        Steps (every guard fails CLOSED to ``None`` so the caller keeps the
        strategy-reported fallback rather than fabricating a value, in the SAME
        order both methods used before unification):

        1. Resolve the wallet from ``details`` (wallet / wallet_address / owner).
        2. Read the wallet's open positions for ``position.protocol`` via the
           gateway-routed reader; an empty book → ``None``.
        3. Match by (market, is_long, collateral_token) — ``is_long`` is
           money-critical and never defaulted (missing → ``None``).
        4. Resolve index-token + collateral metadata through
           :class:`PerpsReadRegistry` (the framework names no venue).
        5. Price the index (mark) + collateral through ``market`` (non-positive
           price → ``None``); resolve collateral decimals (unknown → ``None``).
        6. Mark-to-market via the connector's pure ``value_position`` formula
           (``collateral + unrealized_pnl - pending_fees``).

        Returns the connector's :class:`PerpsPositionValue`, or ``None`` on any
        miss/failure (the whole body is wrapped in the same fail-closed
        ``try/except`` both methods carried).
        """
        try:
            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            if not wallet_address:
                return None

            # Query all positions for this wallet.
            result = self._perps_reader.read_positions(chain, wallet_address, position.protocol)
            on_chain_positions = result.positions
            if not on_chain_positions:
                return None

            # Match position by market address and direction. Accept both the
            # ``market`` and legacy ``market_address`` detail shapes, mirroring
            # ``_canonical_position_key`` so a strategy-reported perp keyed under
            # the legacy shape still reprices (instead of silently keeping its
            # stale/zero fallback value).
            market_address = (position.details.get("market") or position.details.get("market_address") or "").lower()
            if "is_long" not in position.details:
                # Direction is money-critical — never assume long/short.
                return None
            is_long = position.details["is_long"]
            collateral_token = position.details.get("collateral_token", "").lower()

            matched = None
            for ocp in on_chain_positions:
                if ocp.market.lower() == market_address and ocp.is_long == is_long:
                    # If collateral token specified, match it too.
                    if collateral_token and ocp.collateral_token.lower() != collateral_token:
                        continue
                    matched = ocp
                    break

            if matched is None:
                logger.debug(
                    "No matching perp position found for %s (market=%s, is_long=%s)",
                    position.position_id,
                    market_address,
                    is_long,
                )
                return None

            # Resolve index-token symbol + decimals via the connector's metadata.
            meta = PerpsReadRegistry.market_metadata(position.protocol, matched.market, chain)
            if meta is None:
                return None

            try:
                mark_price = Decimal(str(market.price(meta.index_token_symbol)))
            except Exception:
                logger.debug("Could not get mark price for %s", meta.index_token_symbol)
                return None
            if mark_price <= 0:
                return None

            # Resolve collateral token price.
            collateral_symbol = self._resolve_token_symbol(matched.collateral_token, position, "collateral_token")
            if not collateral_symbol:
                return None

            try:
                collateral_price = Decimal(str(market.price(collateral_symbol)))
            except Exception:
                logger.debug("Could not get collateral price for %s", collateral_symbol)
                return None
            if collateral_price <= 0:
                return None

            # Get collateral token decimals (meta guarantees the index decimals).
            collateral_decimals = self._get_token_decimals(collateral_symbol, chain)
            if collateral_decimals is None:
                return None

            # Compute mark-to-market value via the connector's pure formula.
            # Note: pending funding/borrowing fees are NOT included yet —
            # computing them requires cumulative rate data from DataStore.
            # Net value is therefore an upper bound (fees would reduce it).
            valued = PerpsReadRegistry.value_position(
                position.protocol,
                size_in_usd=matched.size_in_usd,
                size_in_tokens=matched.size_in_tokens,
                collateral_amount=matched.collateral_amount,
                is_long=matched.is_long,
                mark_price_usd=mark_price,
                collateral_token_price_usd=collateral_price,
                collateral_token_decimals=collateral_decimals,
                index_token_decimals=meta.index_token_decimals,
                market=matched.market,
            )
            if valued is None:
                return None

            logger.debug(
                "Perps re-priced: position=%s value=$%s (size=$%s pnl=$%s fees=$%s leverage=%sx)",
                position.position_id,
                valued.net_value_usd,
                valued.size_usd,
                valued.unrealized_pnl_usd,
                valued.pending_fees_usd,
                valued.leverage,
            )
            return valued

        except Exception:
            logger.debug("Perps on-chain re-pricing failed for %s", position.position_id, exc_info=True)
            return None

    def _reprice_perps_on_chain_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any]] | None:
        """Re-price perps position and return enriched details.

        Thin wrapper over :meth:`_value_matched_perp`: returns the same net value
        as :meth:`_reprice_perps_on_chain` plus the per-position breakdown dict
        persisted on the snapshot (``valuation_source="on_chain"``). ``None``
        signals fallback needed.
        """
        valued = self._value_matched_perp(position, chain, market)
        if valued is None:
            return None

        enriched = {
            "market": valued.market,
            "is_long": valued.is_long,
            "size_usd": str(valued.size_usd),
            "collateral_value_usd": str(valued.collateral_value_usd),
            "entry_price_usd": str(valued.entry_price_usd),
            "mark_price_usd": str(valued.mark_price_usd),
            "unrealized_pnl_usd": str(valued.unrealized_pnl_usd),
            "pending_fees_usd": str(valued.pending_fees_usd),
            "leverage": str(valued.leverage),
            "valuation_source": "on_chain",
        }
        return valued.net_value_usd, enriched

    def _reprice_lp_on_chain(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> Decimal | None:
        """Attempt to re-price an LP position using on-chain V3 math.

        Queries the NonfungiblePositionManager for full position data
        (tick range, liquidity), then calculates token amounts and
        prices them with live market data.

        Returns:
            USD value if successful, None to signal fallback needed.
        """
        try:
            token_id = self._extract_token_id(position)
            if token_id is None:
                return None

            on_chain = self._lp_reader.read_position(
                chain=chain,
                token_id=token_id,
                protocol=position.protocol,
            )
            if on_chain is None:
                return None

            if on_chain.liquidity == 0 and on_chain.tokens_owed0 == 0 and on_chain.tokens_owed1 == 0:
                return Decimal("0")

            pricing = self._get_lp_token_pricing(on_chain, position, chain, market)
            if pricing is None:
                return None
            token0_price, token1_price, token0_decimals, token1_decimals = pricing

            current_tick, sqrt_price_x96 = self._resolve_lp_current_tick(
                position,
                chain,
                token0_price,
                token1_price,
                token0_decimals,
                token1_decimals,
            )

            lp_value = value_lp_position(
                liquidity=on_chain.liquidity,
                tick_lower=on_chain.tick_lower,
                tick_upper=on_chain.tick_upper,
                current_tick=current_tick,
                token0_price_usd=token0_price,
                token1_price_usd=token1_price,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                sqrt_price_x96=sqrt_price_x96,
            )

            fees_usd = self._compute_lp_uncollected_fees_usd(
                on_chain,
                token0_price,
                token1_price,
                token0_decimals,
                token1_decimals,
            )

            total = lp_value.value_usd + fees_usd

            logger.debug(
                "LP re-priced: position=%s value=$%s (lp=$%s fees=$%s) in_range=%s",
                position.position_id,
                total,
                lp_value.value_usd,
                fees_usd,
                lp_value.in_range,
            )

            return total

        except Exception:
            logger.debug("LP on-chain re-pricing failed for %s", position.position_id, exc_info=True)
            return None

    def _get_lp_token_pricing(
        self,
        on_chain: "LPPositionOnChain",
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, Decimal, int, int] | None:
        """Resolve LP token symbols, live prices, and decimals.

        Returns ``(token0_price, token1_price, token0_decimals, token1_decimals)``
        or ``None`` when any sub-step degrades (matches §7.5 ValueConfidence —
        downgrade rather than fabricate).
        """
        token0_symbol = self._resolve_token_symbol(on_chain.token0, position, "token0")
        token1_symbol = self._resolve_token_symbol(on_chain.token1, position, "token1")
        if not token0_symbol or not token1_symbol:
            return None

        try:
            token0_price = Decimal(str(market.price(token0_symbol)))
            token1_price = Decimal(str(market.price(token1_symbol)))
        except Exception:
            logger.debug("Could not get prices for LP tokens %s/%s", token0_symbol, token1_symbol)
            return None

        if token0_price <= 0 or token1_price <= 0:
            return None

        token0_decimals = self._get_token_decimals(token0_symbol, chain)
        token1_decimals = self._get_token_decimals(token1_symbol, chain)
        if token0_decimals is None or token1_decimals is None:
            logger.debug("Unknown decimals for LP tokens %s/%s, falling back", token0_symbol, token1_symbol)
            return None

        return token0_price, token1_price, token0_decimals, token1_decimals

    def _resolve_lp_current_tick(
        self,
        position: "PositionInfo",
        chain: str,
        token0_price: Decimal,
        token1_price: Decimal,
        token0_decimals: int,
        token1_decimals: int,
    ) -> tuple[int, int | None]:
        """Resolve ``(current_tick, sqrt_price_x96)`` for V3 valuation.

        Prefers exact ``slot0`` (mid-tick precision in narrow ranges); falls
        back to price-ratio derivation when ``pool_address`` is missing OR the
        slot0 read fails. ``sqrt_price_x96`` stays ``None`` on the fallback
        path so ``value_lp_position`` uses tick math.
        """
        pool_address = _resolve_lp_pool_address_from_details(position)
        if pool_address:
            slot0 = self._lp_reader.read_pool_slot0(chain, pool_address)
            if slot0:
                return slot0.tick, slot0.sqrt_price_x96

        derived_tick = self._price_ratio_to_tick(
            token0_price,
            token1_price,
            token0_decimals,
            token1_decimals,
        )
        return derived_tick, None

    @staticmethod
    def _compute_lp_uncollected_fees_usd(
        on_chain: "LPPositionOnChain",
        token0_price: Decimal,
        token1_price: Decimal,
        token0_decimals: int,
        token1_decimals: int,
    ) -> Decimal:
        fees_usd = Decimal("0")
        if on_chain.tokens_owed0 > 0:
            fees_usd += Decimal(on_chain.tokens_owed0) / Decimal(10**token0_decimals) * token0_price
        if on_chain.tokens_owed1 > 0:
            fees_usd += Decimal(on_chain.tokens_owed1) / Decimal(10**token1_decimals) * token1_price
        return fees_usd

    def _reprice_lending_on_chain(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> Decimal | None:
        """Attempt to re-price a lending position using on-chain Aave V3 data.

        Queries getUserReserveData for the position's asset and calculates
        supply value and/or debt value using live prices.

        For SUPPLY positions: returns supply_value - debt_value (net).
        For BORROW positions: returns negative debt_value_usd so it
        reduces the portfolio total when summed.

        Returns:
            USD value if successful, None to signal fallback needed.
        """
        from almanak.framework.teardown.models import PositionType

        try:
            # Need asset address and wallet address
            asset_address = self._extract_asset_address(position)

            # Fallback: resolve asset address from symbol via TokenResolver
            if not asset_address:
                asset_symbol = position.details.get("asset")
                if asset_symbol:
                    try:
                        from almanak.framework.data.tokens import get_token_resolver

                        resolved = get_token_resolver().resolve(asset_symbol, chain)
                        if resolved and resolved.address:
                            asset_address = resolved.address
                    except Exception:
                        pass

            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            if not asset_address or not wallet_address:
                return None

            # Query on-chain position
            on_chain = self._lending_reader.read_position(
                chain=chain,
                asset_address=asset_address,
                wallet_address=wallet_address,
                protocol=position.protocol,
            )
            if on_chain is None:
                return None

            # No supply and no debt = truly empty
            if not on_chain.is_active:
                return Decimal("0")

            # Resolve token symbol for pricing
            token_symbol = self._resolve_token_symbol(on_chain.asset_address, position, "asset")
            if not token_symbol:
                # Try the asset field directly
                token_symbol = position.details.get("asset")
            if not token_symbol:
                return None

            # Get live price
            try:
                token_price = Decimal(str(market.price(token_symbol)))
            except Exception:
                logger.debug("Could not get price for lending token %s", token_symbol)
                return None

            if token_price <= 0:
                return None

            # Get token decimals
            token_decimals = self._get_token_decimals(token_symbol, chain)
            if token_decimals is None:
                logger.debug("Unknown decimals for lending token %s, falling back", token_symbol)
                return None

            # Calculate value
            valued = value_lending_position(
                atoken_balance=on_chain.current_atoken_balance,
                stable_debt=on_chain.current_stable_debt,
                variable_debt=on_chain.current_variable_debt,
                token_price_usd=token_price,
                token_decimals=token_decimals,
                collateral_enabled=on_chain.usage_as_collateral_enabled,
                asset=token_symbol,
            )

            # For SUPPLY positions: return net value (supply - debt).
            # For BORROW positions: return negative debt value so it
            # reduces the portfolio total when summed in _get_positions.
            # USD legs are MeasuredMoney (VIB-5216); price is guarded measured
            # above so ``.value_or`` returns the real value here (byte-identical).
            if position.position_type == PositionType.BORROW:
                result = (-valued.debt_value_usd).value_or(Decimal("0"))
            else:
                result = valued.net_value_usd.value_or(Decimal("0"))

            logger.debug(
                "Lending re-priced: position=%s type=%s value=$%s (supply=$%s debt=$%s) collateral=%s",
                position.position_id,
                position.position_type.value,
                result,
                valued.supply_value_usd.value_or(Decimal("0")),
                valued.debt_value_usd.value_or(Decimal("0")),
                valued.collateral_enabled,
            )

            return result

        except Exception:
            logger.debug(
                "Lending on-chain re-pricing failed for %s",
                position.position_id,
                exc_info=True,
            )
            return None

    def _reprice_perps_on_chain(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> Decimal | None:
        """Attempt to re-price a perp position using on-chain data.

        Thin wrapper over :meth:`_value_matched_perp`: queries the wallet's open
        positions for ``position.protocol``, matches by market + direction, and
        marks to market via the connector's pure formula
        (``collateral + unrealized_pnl - pending_fees``).

        Returns:
            Net USD value if successful, None to signal fallback needed.
        """
        valued = self._value_matched_perp(position, chain, market)
        return valued.net_value_usd if valued is not None else None

    def _reprice_vault_on_chain(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> Decimal | None:
        """Attempt to re-price an ERC-4626 vault position using on-chain data.

        Reads share balance via the vault registry and converts to underlying
        asset amount using the vault's PPFS / convertToAssets. Closes the
        silent zero-valuation gap that today affects MetaMorpho positions.

        Returns USD value if successful, None to signal fallback needed.
        """
        result = self._reprice_vault_on_chain_enriched(position, chain, market)
        if result is None:
            return None
        return result[0]

    def _reprice_lp_enriched_dispatch(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any], bool]:
        """Dispatch an LP position to the correct enriched repricing path.

        Three LP families, gated by capability/data-shape (VIB-4636 discipline),
        never by a hardcoded protocol name:

        * **Uniswap V4** (VIB-5018 / VIB-4586): isolated identity-faithful path.
          MUST NOT fall through to the V3 ``positions(uint256)`` read, which
          corrupts a V4 tokenId into a wrong pool / 10^7 amounts (the $289M bug).
          Detected by data shape (64-hex PoolKey hash vs 40-hex V3 pool address).
        * **Fungible two-token LP** (VIB-5032 — Fluid SmartLending): valued from
          the connector resolver (share balance → token0/token1), NOT the V3-NFT
          path. Capability-gated by the registered fungible-LP reader. A failed
          resolver read fails closed (``repriced=False`` → UNAVAILABLE), never a
          fabricated zero.
        * **Uniswap V3 / Aerodrome CL**: the V3-shaped on-chain read. VIB-4584 /
          F3.1: only flag ``no_path`` when no value source exists anywhere —
          a strategy-reported ``value_usd > 0`` is a value we trust.
        """
        if self._is_v4_lp_position(position):
            return self._reprice_v4_lp_enriched(position, chain, market)

        if self._fungible_lp_reader.supports(position.protocol):
            result = self._reprice_fungible_lp_enriched(position, chain, market)
            if result is not None:
                return result[0], result[1], True
            return position.value_usd, {}, False

        result = self._reprice_lp_on_chain_enriched(position, chain, market)
        if result is not None:
            return result[0], result[1], True
        if position.value_usd > 0:
            return position.value_usd, {}, True
        return position.value_usd, {}, False

    def _reprice_fungible_lp_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any]] | None:
        """Re-price a fungible two-token LP (Fluid SmartLending) — VIB-5032.

        Value = each leg (share→token amount) priced via the oracle and summed.
        Returns ``None`` on any failure (Empty ≠ Zero — the caller flags
        ``no_path``/``UNAVAILABLE``); returns a measured ``Decimal("0")`` only
        for a genuinely empty (zero-share) position.
        """
        try:
            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            wrapper = (
                position.details.get("pool_address") or position.details.get("wrapper") or position.details.get("pool")
            )
            protocol = position.protocol
            if not wallet_address or not wrapper or not protocol:
                return None

            on_chain = self._fungible_lp_reader.read_position(
                protocol=protocol,
                chain=chain,
                wrapper=wrapper,
                wallet_address=wallet_address,
            )
            if on_chain is None:
                return None
            if not on_chain.is_active:
                return Decimal("0"), {"wrapper": wrapper, "shares_wei": "0"}

            def _price_leg(address: str, symbol: str) -> Decimal | None:
                # Price by ADDRESS first — the gateway oracle only builds a
                # ``ResolvedToken`` (which engages CoinGecko's by-address endpoint
                # and DexScreener's by-address path) for address-form inputs; a
                # bare symbol skips those sources. Fall back to the symbol if no
                # address is available. Empty≠Zero: a miss raises → None (caller
                # flags UNAVAILABLE), never a fabricated zero.
                for key in (address, symbol):
                    if not key:
                        continue
                    try:
                        priced = Decimal(str(market.price(key)))
                    except Exception:  # noqa: BLE001 — try the next key, else None
                        continue
                    # Fail closed on a non-positive price (≤ 0): a real token leg
                    # is never worth ≤ $0, so a 0/negative is an oracle miss, not a
                    # measured value. Returning it would underprice the LP while
                    # marking it repriced AND skip the symbol fallback — so keep
                    # trying the next key, else None (UNAVAILABLE, Empty≠Zero).
                    if priced > 0:
                        return priced
                return None

            price0 = _price_leg(on_chain.token0_address, on_chain.token0_symbol)
            price1 = _price_leg(on_chain.token1_address, on_chain.token1_symbol)
            if price0 is None or price1 is None:
                logger.debug(
                    "Could not price fungible-LP legs %s/%s (addr %s/%s)",
                    on_chain.token0_symbol,
                    on_chain.token1_symbol,
                    on_chain.token0_address,
                    on_chain.token1_address,
                )
                return None

            amount0 = Decimal(on_chain.amount0_wei) / Decimal(10**on_chain.token0_decimals)
            amount1 = Decimal(on_chain.amount1_wei) / Decimal(10**on_chain.token1_decimals)
            value_usd = amount0 * price0 + amount1 * price1
            if value_usd <= 0:
                return None

            details: dict[str, Any] = {
                "wrapper": wrapper,
                "shares_wei": str(on_chain.shares_wei),
                # For a fungible LP the wrapper SHARE balance IS the liquidity
                # measure (no tick-bracketed concentrated liquidity). Stamp it on
                # the Track-C snapshot so "liquidity over time" (Accountant LP6)
                # reads a real value instead of an empty concentrated-LP field.
                "liquidity": str(on_chain.shares_wei),
                "token0_symbol": on_chain.token0_symbol,
                "token1_symbol": on_chain.token1_symbol,
                "amount0": str(amount0),
                "amount1": str(amount1),
                "amount0_wei": str(on_chain.amount0_wei),
                "amount1_wei": str(on_chain.amount1_wei),
                "token0_price_usd": str(price0),
                "token1_price_usd": str(price1),
            }
            logger.debug(
                "Fungible-LP re-priced: position=%s protocol=%s value=$%s (shares=%s %s/%s)",
                position.position_id,
                protocol,
                value_usd,
                on_chain.shares_wei,
                on_chain.token0_symbol,
                on_chain.token1_symbol,
            )
            return value_usd, details
        except Exception:
            logger.debug("Fungible-LP on-chain re-pricing failed for %s", position.position_id, exc_info=True)
            return None

    def _reprice_vault_on_chain_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any]] | None:
        """Re-price a vault position and return enriched details for snapshots."""
        try:
            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            vault_address = position.details.get("vault_address") or position.details.get("vault")
            protocol = position.protocol
            if not wallet_address or not vault_address or not protocol:
                return None

            on_chain = self._vault_reader.read_position(
                protocol=protocol,
                chain=chain,
                vault_address=vault_address,
                wallet_address=wallet_address,
            )
            if on_chain is None:
                return None

            if not on_chain.is_active:
                return Decimal("0"), {
                    "vault_address": vault_address,
                    "shares_wei": "0",
                    "asset_amount_wei": "0",
                }

            # Resolve underlying asset symbol for pricing
            asset_symbol = self._resolve_token_symbol(on_chain.asset_address, position, "asset")
            if not asset_symbol:
                asset_symbol = position.details.get("asset")
            if not asset_symbol:
                logger.debug(
                    "Vault re-pricing: cannot resolve asset symbol for %s (asset=%s)",
                    position.position_id,
                    on_chain.asset_address,
                )
                return None

            try:
                asset_price = Decimal(str(market.price(asset_symbol)))
            except Exception:
                logger.debug("Could not get price for vault asset %s", asset_symbol)
                return None

            if asset_price <= 0:
                return None

            asset_decimals = on_chain.asset_decimals
            if asset_decimals <= 0:
                # Defensive: fall back to token resolver if the on-chain decimals() read returned 0.
                resolved = self._get_token_decimals(asset_symbol, chain)
                if resolved is None:
                    return None
                asset_decimals = resolved

            asset_amount = Decimal(on_chain.asset_amount_wei) / Decimal(10**asset_decimals)
            value_usd = asset_amount * asset_price

            details: dict[str, Any] = {
                "vault_address": vault_address,
                "asset_address": on_chain.asset_address,
                "asset_symbol": asset_symbol,
                "shares_wei": str(on_chain.shares_wei),
                "asset_amount_wei": str(on_chain.asset_amount_wei),
                "asset_amount": str(asset_amount),
                "asset_price_usd": str(asset_price),
            }

            logger.debug(
                "Vault re-priced: position=%s protocol=%s value=$%s (shares=%s assets=%s %s)",
                position.position_id,
                protocol,
                value_usd,
                on_chain.shares_wei,
                asset_amount,
                asset_symbol,
            )

            return value_usd, details

        except Exception:
            logger.debug(
                "Vault on-chain re-pricing failed for %s",
                position.position_id,
                exc_info=True,
            )
            return None

    @staticmethod
    def _extract_asset_address(position: "PositionInfo") -> str | None:
        """Extract the underlying asset address from position details."""
        for key in ("asset_address", "assetAddress", "token_address", "underlying"):
            val = position.details.get(key)
            if val and isinstance(val, str) and len(val) >= 40:
                return val
        return None

    @staticmethod
    def _extract_token_id(position: "PositionInfo") -> int | None:
        """Extract numeric NFT token ID from position data."""
        pid = position.position_id
        if not pid:
            return None

        # Try direct numeric parse
        try:
            token_id = int(pid)
            if token_id >= 0:
                return token_id
        except (ValueError, TypeError):
            pass

        # Check details dict
        for key in ("token_id", "tokenId", "nft_id"):
            val = position.details.get(key)
            if val is not None:
                try:
                    return int(val)
                except (ValueError, TypeError):
                    pass

        return None

    @staticmethod
    def _resolve_token_symbol(
        token_address: str,
        position: "PositionInfo",
        field_name: str,
    ) -> str | None:
        """Resolve a token address to a symbol.

        Prefers the authoritative on-chain address via TokenResolver,
        then falls back to strategy-reported metadata.
        """
        # Primary: resolve from on-chain address (authoritative)
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            resolved = resolver.resolve(token_address, position.chain)
            if resolved and resolved.symbol:
                return resolved.symbol
        except Exception:
            pass

        # Fallback: strategy-reported metadata
        symbol = position.details.get(field_name)
        if symbol:
            return symbol

        # Fallback: tokens list (LP-specific — only valid for token0/token1)
        if field_name in ("token0", "token1"):
            tokens = position.details.get("tokens", [])
            idx = 0 if field_name == "token0" else 1
            if len(tokens) > idx:
                return tokens[idx]

        return None

    @staticmethod
    def _get_token_decimals(symbol: str, chain: str) -> int | None:
        """Get token decimals. Returns None if unknown (never defaults to 18).

        Per codebase rules: "NEVER default to 18 decimals -- always raise
        TokenNotFoundError if decimals unknown."
        """
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            return resolver.get_decimals(chain, symbol)
        except Exception:
            return None

    def _enrich_position_pnl(
        self,
        position_value: PositionValue,
        position_info: "PositionInfo",
        chain: str,
    ) -> None:
        """Populate cost_basis_usd, unrealized_pnl_usd, realized_pnl_usd, and timestamps
        from stored accounting or position events. Best-effort: silently skips on any failure.

        Enrichment paths by position type:
        - SUPPLY / BORROW: accounting_events table keyed by lending position_key.
        - LP: position_events table, OPEN event keyed by NFT position_id.
        - PERP: position_events table, OPEN event keyed by position_id.
        - VAULT: accounting_events table keyed by vault position_key (VAULT_DEPOSIT events).
        """
        if not self._accounting_store or not self._deployment_id:
            return
        try:
            from almanak.framework.teardown.models import PositionType

            if position_info.position_type in (PositionType.SUPPLY, PositionType.BORROW):
                self._enrich_lending_pnl(position_value, position_info, chain)
            elif position_info.position_type == PositionType.LP:
                self._enrich_lp_pnl(position_value, position_info)
            elif position_info.position_type == PositionType.PERP:
                self._enrich_perp_pnl(position_value, position_info)
            elif position_info.position_type == PositionType.VAULT:
                self._enrich_vault_pnl(position_value, position_info, chain)
        except Exception:
            logger.debug("_enrich_position_pnl failed for %s", position_info.position_id, exc_info=True)

    def _enrich_lending_pnl(
        self,
        position_value: PositionValue,
        position_info: "PositionInfo",
        chain: str,
    ) -> None:
        """Enrich SUPPLY/BORROW positions from accounting_events (existing path)."""
        from almanak.framework.accounting.position_pnl import compute_position_pnl
        from almanak.framework.teardown.models import PositionType

        position_key = self._try_derive_lending_position_key(position_info, chain)
        if not position_key:
            return

        # VIB-3503 Part 2c: read from the per-snapshot prefetch cache when
        # available; fall back to a per-position lookup for callers that
        # bypass value().
        events = self._events_for_position_key(position_key)
        if not events:
            # VIB-4085 / VIB-3917 — accounting_events lookup may miss when
            # the BORROW just landed and the outbox processor hasn't
            # flushed it to the table yet (snapshot fires same iteration
            # as the BORROW write). Fall back to the in-memory
            # ``_recent_open_events`` cache populated synchronously by
            # ``save_position_event`` (Layer 3 is wired without an
            # outbox). Without this, the snapshot row carries
            # ``value_confidence=HIGH`` AND a synthesised position with
            # ``cost_basis_usd=null`` — exactly the contract VIB-3917 G6
            # forbids.
            self._enrich_lending_pnl_from_open_event(position_value, position_info, chain)
            return

        # P2 fix: filter events to those relevant for this position side so that a
        # SUPPLY and BORROW for the same wallet/protocol/asset do not cross-contaminate.
        # The accounting position_key omits the lending side, so both sides share a key.
        if position_info.position_type == PositionType.BORROW:
            # DELEVERAGE closes/reduces a borrow through the same
            # ``match_repay`` path as REPAY (VIB-4974) and carries debt-side
            # principal + interest. It must be included here or a deleveraged
            # unwind's realized cost is silently dropped from
            # ``cost_basis_usd`` / ``realized_pnl_usd`` on the snapshot lane.
            relevant_event_types = {"BORROW", "REPAY", "DELEVERAGE"}
        else:
            relevant_event_types = {"SUPPLY", "WITHDRAW"}
        events = [e for e in events if e.get("event_type") in relevant_event_types]
        if not events:
            return

        pnl = compute_position_pnl(events)
        if pnl is None:
            return

        position_value.cost_basis_usd = pnl.cost_basis_usd
        # P1 fix: BORROW positions carry a negative value_usd (liability semantics).
        # unrealized_pnl = value_usd + cost_basis_usd
        #                = (-current_debt) + outstanding_principal
        #                = -(accrued_interest)   [negative when interest has accrued]
        # SUPPLY positions use the standard asset formula: value - cost_basis.
        if position_info.position_type == PositionType.BORROW:
            position_value.unrealized_pnl_usd = position_value.value_usd + pnl.cost_basis_usd
        else:
            # Always compute for SUPPLY even when cost_basis is zero: a position
            # that has fully recovered principal still has unrealized PnL = value_usd.
            position_value.unrealized_pnl_usd = position_value.value_usd - pnl.cost_basis_usd
        position_value.realized_pnl_usd = pnl.realized_pnl_usd
        position_value.entry_timestamp = pnl.entry_timestamp
        position_value.last_update_timestamp = pnl.latest_timestamp
        position_value.ledger_entry_id = pnl.latest_ledger_entry_id

    @staticmethod
    def _resolve_lending_wallet_and_asset(position_info: "PositionInfo") -> tuple[str, str]:
        """Pluck the lending wallet/asset out of details, tolerant to the
        three legacy field names (wallet / wallet_address / owner)."""
        details = position_info.details
        wallet = details.get("wallet") or details.get("wallet_address") or details.get("owner") or ""
        asset = details.get("asset") or ""
        return wallet, asset

    def _lookup_open_event_cost_basis(
        self,
        position_id: str,
        position_type_str: str,
    ) -> tuple[Decimal, dict] | None:
        """Read the OPEN event from ``_recent_open_events`` and parse its
        ``value_usd`` into a positive Decimal. Returns ``None`` for any
        miss (no cache, no entry, unparseable value, non-positive)."""
        cache = getattr(self, "_recent_open_events", None) or {}
        cached = cache.get((position_id, position_type_str))
        if cached is None:
            return None
        try:
            cost_basis = Decimal(str(cached.get("value_usd") or "0"))
        except Exception:  # noqa: BLE001
            return None
        if cost_basis <= Decimal("0"):
            return None
        return cost_basis, cached

    def _enrich_lending_pnl_from_open_event(
        self,
        position_value: PositionValue,
        position_info: "PositionInfo",
        chain: str,
    ) -> None:
        """VIB-4085 / VIB-3917 — same-iteration fallback for lending cost
        basis when the accounting_events outbox hasn't flushed yet.

        Layer 5 (``accounting_events``) writes go through the outbox +
        async processor (VIB-3467); Layer 3 (``position_events``) writes
        are synchronous. So when a snapshot fires in the same iteration
        as a SUPPLY/BORROW, the position_events OPEN row is on disk and
        the runner's ``_recent_open_events`` cache has it, but the
        accounting_events row doesn't exist yet. Reading
        ``value_usd`` off the OPEN event is exactly the cost-basis
        semantics the SUPPLY/BORROW principal carries: USD value of the
        capital deployed at the moment the position opened.

        The accounting_events path remains preferred because it carries
        the principal/interest split that ``compute_position_pnl``
        derives. This fallback only runs when that path returns no
        events at all.
        """
        from almanak.framework.observability.position_events import lending_position_id
        from almanak.framework.teardown.models import PositionType

        wallet, asset = self._resolve_lending_wallet_and_asset(position_info)
        if not wallet or not asset or not chain:
            return

        # VIB-4981 — market_id scopes isolated-lending (Morpho Blue) positions.
        # The L3 OPEN row keyed by lending_position_id now carries market_id, so
        # this same-iteration cost-basis lookup MUST pass it too or it would
        # re-introduce the L3/L5 asymmetry on the valuer path and miss the OPEN
        # event for Morpho positions. ``details["market_id"]`` is the canonical
        # source — the sibling _try_derive_lending_position_key reads the same
        # field. Absent (Aave-style) ⇒ None ⇒ no extra segment ⇒ key unchanged.
        market_id = position_info.details.get("market_id")

        position_id = lending_position_id(
            chain=chain,
            protocol=position_info.protocol or "",
            wallet=wallet,
            asset=asset,
            market_id=market_id,
        )
        is_debt = position_info.position_type == PositionType.BORROW
        position_type_str = "LENDING_DEBT" if is_debt else "LENDING_COLLATERAL"

        lookup = self._lookup_open_event_cost_basis(position_id, position_type_str)
        if lookup is None:
            return
        cost_basis, cached = lookup

        position_value.cost_basis_usd = cost_basis
        # BORROW is a liability (value_usd negative), SUPPLY is an asset —
        # signage mirrors _enrich_lending_pnl.
        if is_debt:
            position_value.unrealized_pnl_usd = position_value.value_usd + cost_basis
        else:
            position_value.unrealized_pnl_usd = position_value.value_usd - cost_basis

        ts = cached.get("timestamp") or ""
        if isinstance(ts, str):
            position_value.entry_timestamp = ts
        ledger_id = cached.get("ledger_entry_id") or ""
        if ledger_id:
            position_value.ledger_entry_id = ledger_id

    def _enrich_lp_pnl(
        self,
        position_value: PositionValue,
        position_info: "PositionInfo",
    ) -> None:
        """Enrich LP positions from the position_events table.

        Looks up the earliest OPEN event for this position_id and extracts
        value_usd as cost_basis_usd (= USD value at the time the position
        was opened).  If no OPEN event exists, leaves cost_basis_usd = 0.
        """
        self._enrich_from_open_event(position_value, position_info, position_type="LP")

    def _enrich_perp_pnl(
        self,
        position_value: PositionValue,
        position_info: "PositionInfo",
    ) -> None:
        """Enrich PERP positions from the position_events table.

        Looks up the earliest OPEN event for this position_id.  The
        value_usd on the OPEN event is the initial collateral/notional
        deployed, used as cost_basis_usd.
        """
        self._enrich_from_open_event(position_value, position_info, position_type="PERP")

    def _enrich_from_open_event(
        self,
        position_value: PositionValue,
        position_info: "PositionInfo",
        position_type: str,
    ) -> None:
        """Shared helper: enrich a position from its earliest OPEN event in position_events.

        Reads the first OPEN event for the given position_id and position_type, then
        populates cost_basis_usd, unrealized_pnl_usd, entry_timestamp, and
        ledger_entry_id on the PositionValue.  No-op when no matching event exists.

        VIB-3894: a runner-side ``_recent_open_events`` cache (populated when
        ``save_position_event`` succeeds for an OPEN event) is consulted first
        so the same-iteration snapshot fired right after LP_OPEN sees the
        cost basis even when the underlying ``state_manager`` doesn't expose
        ``get_position_events_sync`` (canonical case for ``GatewayStateManager``).

        Args:
            position_value: The PositionValue to enrich (mutated in place).
            position_info: Source PositionInfo carrying the position_id.
            position_type: Value passed to get_position_events_sync (e.g. "LP", "PERP").
        """
        position_id = position_info.position_id
        if not position_id:
            return

        # VIB-3894 — recent-open cache lookup (in-memory, runner-side).
        cache = getattr(self, "_recent_open_events", None) or {}
        cached = cache.get((str(position_id), position_type))
        if cached is not None:
            try:
                cost_basis = Decimal(str(cached.get("value_usd") or "0"))
            except Exception:
                cost_basis = Decimal("0")
            if cost_basis > Decimal("0"):
                position_value.cost_basis_usd = cost_basis
                position_value.unrealized_pnl_usd = position_value.value_usd - cost_basis
                ts = cached.get("timestamp") or ""
                if isinstance(ts, str):
                    position_value.entry_timestamp = ts
                ledger_id = cached.get("ledger_entry_id") or ""
                if ledger_id:
                    position_value.ledger_entry_id = ledger_id
                return

        if not hasattr(self._accounting_store, "get_position_events_sync"):
            return

        events = self._accounting_store.get_position_events_sync(
            self._deployment_id,
            position_id=position_id,
            position_type=position_type,
            event_type="OPEN",
        )
        if not events:
            return

        # Events are returned ASC; the first is the earliest OPEN.
        open_event = events[0]
        value_usd_raw = open_event.get("value_usd")
        if value_usd_raw is None or value_usd_raw == "":
            return

        try:
            cost_basis = Decimal(str(value_usd_raw))
        except Exception:
            return

        if cost_basis <= Decimal("0"):
            return

        position_value.cost_basis_usd = cost_basis
        position_value.unrealized_pnl_usd = position_value.value_usd - cost_basis
        entry_ts = open_event.get("timestamp") or ""
        if isinstance(entry_ts, str):
            position_value.entry_timestamp = entry_ts
        ledger_id = open_event.get("ledger_entry_id") or ""
        if ledger_id:
            position_value.ledger_entry_id = ledger_id

    def _enrich_vault_pnl(
        self,
        position_value: PositionValue,
        position_info: "PositionInfo",
        chain: str,
    ) -> None:
        """Enrich VAULT positions from the accounting_events table.

        Looks up VAULT_DEPOSIT events for this position's vault+wallet key
        and uses the deposit_usd payload field as cost_basis_usd.
        If no VAULT_DEPOSIT events exist, leaves cost_basis_usd = 0.
        """
        position_key = self._try_derive_vault_position_key(position_info, chain)
        if not position_key:
            return

        # VIB-3503 Part 2c: read from the per-snapshot prefetch cache.
        # Falls back to a per-position lookup for callers that bypass value().
        events = self._events_for_position_key(position_key)
        if not events:
            return

        # Filter to VAULT_DEPOSIT events only
        deposit_events = [e for e in events if e.get("event_type") == "VAULT_DEPOSIT"]
        if not deposit_events:
            return

        # Sum all deposits for the cost basis (similar to SUPPLY logic)
        cost_basis = Decimal("0")
        for ev in deposit_events:
            try:
                payload = json.loads(ev.get("payload_json") or "{}")
            except Exception:
                continue
            # vault_accounting serialises cost_basis_usd; older rows used deposit_usd
            deposit_raw = payload.get("cost_basis_usd") or payload.get("deposit_usd")
            if deposit_raw is None:
                continue
            try:
                cost_basis += Decimal(str(deposit_raw))
            except Exception:
                pass

        if cost_basis <= Decimal("0"):
            return

        sorted_events = sorted(deposit_events, key=lambda e: e.get("timestamp", ""))
        oldest = sorted_events[0]
        latest = sorted_events[-1]

        position_value.cost_basis_usd = cost_basis
        position_value.unrealized_pnl_usd = position_value.value_usd - cost_basis
        entry_ts = oldest.get("timestamp") or ""
        if isinstance(entry_ts, str):
            position_value.entry_timestamp = entry_ts
        ledger_id = latest.get("ledger_entry_id") or ""
        if ledger_id:
            position_value.ledger_entry_id = ledger_id

    @staticmethod
    def _try_derive_lending_position_key(position: "PositionInfo", chain: str) -> str | None:
        """Derive the accounting position_key from a PositionInfo for lending positions.

        Mirrors the logic in lending_accounting._derive_position_key so that
        accounting events written during execution can be matched at snapshot time.
        Returns None for non-lending position types or when required details are absent.
        """
        from almanak.framework.teardown.models import PositionType

        if position.position_type not in (PositionType.SUPPLY, PositionType.BORROW):
            return None
        if not chain:
            return None
        wallet = (
            position.details.get("wallet")
            or position.details.get("wallet_address")
            or position.details.get("owner")
            or ""
        )
        asset = position.details.get("asset") or ""
        if not wallet or not asset:
            return None
        market_id = position.details.get("market_id")
        parts = ["lending", chain.lower(), position.protocol.lower(), wallet.lower()]
        if market_id:
            parts.append(str(market_id).lower())
        parts.append(asset.lower())
        return ":".join(parts)

    @staticmethod
    def _try_derive_vault_position_key(position: "PositionInfo", chain: str) -> str | None:
        """Derive the accounting position_key for a VAULT position.

        The key mirrors whatever the vault accounting writer uses when it records
        VAULT_DEPOSIT events.  For now, the canonical form is:
            vault:<chain>:<protocol>:<wallet_lower>:<vault_address_lower>

        Returns None for non-vault position types or when required details are absent.
        """
        from almanak.framework.teardown.models import PositionType

        if position.position_type != PositionType.VAULT:
            return None
        if not chain:
            return None
        wallet = (
            position.details.get("wallet")
            or position.details.get("wallet_address")
            or position.details.get("owner")
            or ""
        )
        vault_address = position.details.get("vault_address") or position.details.get("vault") or ""
        if not wallet or not vault_address:
            return None
        return ":".join(["vault", chain.lower(), position.protocol.lower(), wallet.lower(), vault_address.lower()])

    @staticmethod
    def _price_ratio_to_tick(
        token0_price: Decimal,
        token1_price: Decimal,
        token0_decimals: int,
        token1_decimals: int,
    ) -> int:
        """Derive approximate V3 tick from USD prices and decimals.

        V3 price = token1_amount / token0_amount (in wei terms).
        tick = log(price) / log(1.0001)
        """
        import math

        if token0_price <= 0 or token1_price <= 0:
            return 0

        # V3 price is token1/token0 in wei terms
        # price = (token0_price / token1_price) * (10^token1_decimals / 10^token0_decimals)
        price_ratio = float(token0_price / token1_price) * (10**token1_decimals / 10**token0_decimals)

        if price_ratio <= 0:
            return 0

        # tick = log(price) / log(1.0001)
        tick = math.log(price_ratio) / math.log(1.0001)
        return int(tick)
