"""Portfolio valuation orchestrator.

Produces PortfolioSnapshot by querying the gateway (via MarketSnapshot)
for wallet balances and token prices, using the PositionDiscoveryService
to proactively find on-chain positions (LP, lending, perps), with
strategy.get_open_positions() as a supplementary source.

This is the single source of truth for portfolio valuation at runtime.
The framework owns both discovery and math.
"""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
from almanak.framework.portfolio.models import (
    PortfolioSnapshot,
    PositionValue,
    TokenBalance,
    ValueConfidence,
)
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.lending_position_reader import LendingPositionReader
from almanak.framework.valuation.lending_valuer import value_lending_position
from almanak.framework.valuation.lp_position_reader import LPPositionReader
from almanak.framework.valuation.lp_valuer import value_lp_position
from almanak.framework.valuation.perps_position_reader import PerpsPositionReader
from almanak.framework.valuation.perps_valuer import value_perps_position
from almanak.framework.valuation.position_discovery import (
    DiscoveryConfig,
    PositionDiscoveryService,
)
from almanak.framework.valuation.spot_valuer import total_value, value_tokens
from almanak.framework.valuation.vault_position_reader import VaultPositionReader

if TYPE_CHECKING:
    from almanak.framework.teardown.models import TeardownPositionSummary
    from almanak.framework.valuation.lp_position_reader import LPPositionOnChain

logger = logging.getLogger(__name__)

FRAMEWORK_EXTERNAL_AGREEMENT_THRESHOLD = Decimal("0.10")
FRAMEWORK_EXTERNAL_DIVERGENCE_THRESHOLD = Decimal("0.20")


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

    Collapses known lending-fork aliases onto their registry-canonical key
    (e.g. ``"aave"`` -> ``"aave_v3"``) so a strategy-reported alias and a
    discovery-stamped canonical name dedup as ONE position instead of
    double-counting. Non-lending or unknown protocols (LP / perp / vault) have
    no lending-read canonical form, so they pass through lowercased — preserving
    existing keying for every non-lending position type.
    """
    canonical = LendingReadRegistry.canonical(protocol)
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

    def set_gateway_client(self, gateway_client: object | None) -> None:
        """Update the gateway client for on-chain queries.

        Called by StrategyRunner once the gateway connection is established.
        """
        self._gateway_client = gateway_client
        self._lp_reader = LPPositionReader(gateway_client)
        self._lending_reader = LendingPositionReader(gateway_client)
        self._perps_reader = PerpsPositionReader.from_gateway_client(gateway_client)
        self._vault_reader.set_gateway_client(gateway_client)
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
            position_value_positive = sum(
                (
                    p.value_usd
                    for p in positions
                    if p.value_usd > 0
                    and not (p.position_type == PositionType.TOKEN and _token_overlaps_wallet_index(p, wallet_index))
                ),
                Decimal("0"),
            )

            # VIB-4909: ``wallet_total_value_usd`` is the operator-facing
            # full-portfolio value (wallet + real protocol positions); wallet
            # pseudo-positions are already counted once in ``wallet_value``.
            non_wallet_position_value = sum(
                (
                    p.value_usd
                    for p in positions
                    if not (p.position_type == PositionType.TOKEN and _token_overlaps_wallet_index(p, wallet_index))
                ),
                Decimal("0"),
            )

            framework_snapshot = PortfolioSnapshot(
                timestamp=now,
                deployment_id=deployment_id,
                total_value_usd=position_value_positive,
                available_cash_usd=wallet_value,
                deployed_capital_usd=deployed_capital_usd,
                wallet_total_value_usd=wallet_value + non_wallet_position_value,
                value_confidence=confidence,
                positions=positions,
                wallet_balances=wallet_balances,
                token_prices=token_price_records,
                chain=chain,
                iteration_number=iteration_number,
                snapshot_metadata={"gas_native_status": gas_native_status},
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
        if not self._accounting_store or not deployment_id:
            self._snapshot_event_cache = None
            return
        if not hasattr(self._accounting_store, "get_accounting_events_sync"):
            self._snapshot_event_cache = None
            return
        # Wrap the entire fetch + cache-build in one try/except so a backend
        # returning None, a non-iterable, or rows that aren't dicts can never
        # leak out of value() as an unhandled exception. Snapshot building is
        # the read-side hot path; we'd rather degrade to no PnL enrichment
        # than crash the snapshot.
        try:
            events = self._accounting_store.get_accounting_events_sync(deployment_id) or []
            cache: dict[str, list[dict]] = {}
            for ev in events:
                if not isinstance(ev, dict):
                    continue
                key = ev.get("position_key") or ""
                cache.setdefault(key, []).append(ev)
            self._snapshot_event_cache = cache
        except Exception:
            logger.debug("Accounting prefetch failed; falling back to per-position lookups", exc_info=True)
            self._snapshot_event_cache = None

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
        discovery_config = self._build_discovery_config(strategy, strategy_positions)
        if discovery_config:
            discovered = self._discovery.discover(discovery_config)
            if discovered.errors:
                discovery_had_errors = True
            discovered_positions = list(discovered.positions)

        # Merge the two sources by canonical identity (VIB-4838): discovery is
        # authoritative for value + on-chain details, the strategy for
        # position_type + domain hints. Degenerate strategy stubs that merely
        # duplicate a discovered position are dropped here so they cannot reach
        # the repricer and trip ``no_path``.
        merged_positions = self._merge_position_sources(strategy_positions, discovered_positions, strategy.chain)

        positions_incomplete = strategy_failed or discovery_had_errors
        if not merged_positions:
            return [], Decimal("0"), positions_incomplete

        # Re-price all positions and enrich details with valuer breakdown
        positions: list[PositionValue] = []
        any_unrepriced = False
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
        """
        from almanak.framework.teardown.models import PositionType as _PT

        # Index discovery by canonical key (discovery wins on collision).
        discovery_by_key: dict[tuple, PositionInfo] = {}
        for d in discovered_positions:
            key = self._canonical_position_key(d, chain)
            if key is not None:
                discovery_by_key[key] = d

        strategy_protocol_types = {(sp.protocol, sp.position_type) for sp in strategy_positions}

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
        for d in discovered_positions:
            if id(d) in consumed_discovery:
                continue
            # Preserve the legacy perp precedence: if the strategy reported a
            # perp for the same protocol, skip the discovered one (the two use
            # different id formats and the strategy read its own on-chain size).
            if d.position_type == _PT.PERP and (d.protocol, _PT.PERP) in strategy_protocol_types:
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

        if position.position_type == PositionType.LP:
            result = self._reprice_lp_on_chain_enriched(position, chain, market)
            if result is not None:
                return result[0], result[1], True
            # No LP path matched (e.g. Aerodrome CL, Uniswap V4) or on-chain
            # read failed. VIB-4584 / F3.1 scope: only flag as "no path"
            # when no value source exists anywhere — strategies that report
            # value_usd > 0 are asserting a value we trust (the overnight
            # matrix specifically hit value_usd == 0 with no on-chain path).
            if position.value_usd > 0:
                return position.value_usd, {}, True
            return position.value_usd, {}, False

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
            # PERPs: trust the strategy fallback — strategies (e.g. GMX V2,
            # Drift) report the position with a meaningful value_usd from
            # their own on-chain reads inside ``get_open_positions``.
            return position.value_usd, {}, True

        if position.position_type == PositionType.VAULT:
            result = self._reprice_vault_on_chain_enriched(position, chain, market)
            if result is not None:
                return result[0], result[1], True
            return position.value_usd, {}, True

        return position.value_usd, {}, True

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

            if position.position_type == PositionType.BORROW:
                result_value = -valued.debt_value_usd
            else:
                result_value = valued.net_value_usd

            enriched = {
                "supply_balance": str(valued.supply_balance),
                "supply_value_usd": str(valued.supply_value_usd),
                "stable_debt_balance": str(valued.stable_debt_balance),
                "variable_debt_balance": str(valued.variable_debt_balance),
                "debt_value_usd": str(valued.debt_value_usd),
                "net_value_usd": str(valued.net_value_usd),
                "collateral_enabled": valued.collateral_enabled,
                "health_factor": str(on_chain.health_factor) if hasattr(on_chain, "health_factor") else None,
                "valuation_source": "on_chain",
            }

            return result_value, enriched

        except Exception:
            logger.debug("Lending enriched re-pricing failed for %s", position.position_id, exc_info=True)
            return None

    def _reprice_perps_on_chain_enriched(  # noqa: C901
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any]] | None:
        """Re-price perps position and return enriched details."""
        try:
            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            if not wallet_address:
                return None

            on_chain_positions = self._perps_reader.read_positions(chain, wallet_address)
            if not on_chain_positions:
                return None

            market_address = position.details.get("market", "").lower()
            if "is_long" not in position.details:
                return None
            is_long = position.details["is_long"]
            collateral_token = position.details.get("collateral_token", "").lower()

            matched = None
            for ocp in on_chain_positions:
                if ocp.market.lower() == market_address and ocp.is_long == is_long:
                    if collateral_token and ocp.collateral_token.lower() != collateral_token:
                        continue
                    matched = ocp
                    break

            if matched is None:
                return None

            index_token_symbol = self._resolve_perps_index_token(matched.market, chain)
            if not index_token_symbol:
                return None

            try:
                mark_price = Decimal(str(market.price(index_token_symbol)))
            except Exception:
                return None
            if mark_price <= 0:
                return None

            collateral_symbol = self._resolve_token_symbol(matched.collateral_token, position, "collateral_token")
            if not collateral_symbol:
                return None

            try:
                collateral_price = Decimal(str(market.price(collateral_symbol)))
            except Exception:
                return None
            if collateral_price <= 0:
                return None

            collateral_decimals = self._get_token_decimals(collateral_symbol, chain)
            index_decimals = self._get_perps_index_decimals(matched.market, chain)
            if collateral_decimals is None or index_decimals is None:
                return None

            valued = value_perps_position(
                size_in_usd=matched.size_in_usd,
                size_in_tokens=matched.size_in_tokens,
                collateral_amount=matched.collateral_amount,
                is_long=matched.is_long,
                mark_price_usd=mark_price,
                collateral_token_price_usd=collateral_price,
                collateral_token_decimals=collateral_decimals,
                index_token_decimals=index_decimals,
                market=matched.market,
            )

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

        except Exception:
            logger.debug("Perps enriched re-pricing failed for %s", position.position_id, exc_info=True)
            return None

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
            if position.position_type == PositionType.BORROW:
                result = -valued.debt_value_usd
            else:
                result = valued.net_value_usd

            logger.debug(
                "Lending re-priced: position=%s type=%s value=$%s (supply=$%s debt=$%s) collateral=%s",
                position.position_id,
                position.position_type.value,
                result,
                valued.supply_value_usd,
                valued.debt_value_usd,
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

    def _reprice_perps_on_chain(  # noqa: C901
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> Decimal | None:
        """Attempt to re-price a GMX V2 perp position using on-chain data.

        Queries the wallet's open GMX V2 positions, matches by market address
        and direction (long/short), then computes mark-to-market value using
        the perps_valuer pure math.

        Returns:
            Net USD value (collateral + unrealized PnL - fees) if successful,
            None to signal fallback needed.
        """
        try:
            # Need wallet address and market info from position details
            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            if not wallet_address:
                return None

            # Query all positions for this wallet
            on_chain_positions = self._perps_reader.read_positions(chain, wallet_address)
            if not on_chain_positions:
                return None

            # Match position by market address and direction
            market_address = position.details.get("market", "").lower()
            if "is_long" not in position.details:
                # Direction is money-critical — never assume long/short
                return None
            is_long = position.details["is_long"]
            collateral_token = position.details.get("collateral_token", "").lower()

            matched = None
            for ocp in on_chain_positions:
                if ocp.market.lower() == market_address and ocp.is_long == is_long:
                    # If collateral token specified, match it too
                    if collateral_token and ocp.collateral_token.lower() != collateral_token:
                        continue
                    matched = ocp
                    break

            if matched is None:
                logger.debug(
                    "No matching GMX V2 position found for %s (market=%s, is_long=%s)",
                    position.position_id,
                    market_address,
                    is_long,
                )
                return None

            # Resolve index token price (mark price)
            index_token_symbol = self._resolve_perps_index_token(matched.market, chain)
            if not index_token_symbol:
                return None

            try:
                mark_price = Decimal(str(market.price(index_token_symbol)))
            except Exception:
                logger.debug("Could not get mark price for %s", index_token_symbol)
                return None

            if mark_price <= 0:
                return None

            # Resolve collateral token price
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

            # Get token decimals
            collateral_decimals = self._get_token_decimals(collateral_symbol, chain)
            index_decimals = self._get_perps_index_decimals(matched.market, chain)
            if collateral_decimals is None or index_decimals is None:
                return None

            # Compute mark-to-market value.
            # Note: pending funding/borrowing fees are NOT included yet —
            # computing them requires cumulative rate data from DataStore.
            # Net value is therefore an upper bound (fees would reduce it).
            valued = value_perps_position(
                size_in_usd=matched.size_in_usd,
                size_in_tokens=matched.size_in_tokens,
                collateral_amount=matched.collateral_amount,
                is_long=matched.is_long,
                mark_price_usd=mark_price,
                collateral_token_price_usd=collateral_price,
                collateral_token_decimals=collateral_decimals,
                index_token_decimals=index_decimals,
                market=matched.market,
            )

            logger.debug(
                "Perps re-priced: position=%s value=$%s (size=$%s pnl=$%s fees=$%s leverage=%sx)",
                position.position_id,
                valued.net_value_usd,
                valued.size_usd,
                valued.unrealized_pnl_usd,
                valued.pending_fees_usd,
                valued.leverage,
            )

            return valued.net_value_usd

        except Exception:
            logger.debug(
                "Perps on-chain re-pricing failed for %s",
                position.position_id,
                exc_info=True,
            )
            return None

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
    def _resolve_perps_index_token(market_address: str, chain: str) -> str | None:
        """Map a GMX V2 market address to its index token symbol.

        Uses the market address tables from the GMX V2 adapter.
        """
        try:
            from almanak.connectors.gmx_v2.adapter import GMX_V2_MARKETS

            markets = GMX_V2_MARKETS.get(chain, {})
            addr_lower = market_address.lower()
            for name, addr in markets.items():
                if addr.lower() == addr_lower:
                    # name is like "ETH/USD" — extract index token
                    return name.split("/")[0]
        except ImportError:
            pass
        return None

    @staticmethod
    def _get_perps_index_decimals(market_address: str, chain: str) -> int | None:
        """Get the index token decimals for a GMX V2 market.

        Uses the decimal table from the GMX V2 adapter.
        Case-insensitive lookup to handle both checksummed and lowercased addresses.
        """
        try:
            from almanak.connectors.gmx_v2.adapter import _GMX_V2_INDEX_TOKEN_DECIMALS

            chain_decimals = _GMX_V2_INDEX_TOKEN_DECIMALS.get(chain, {})
            addr_lower = market_address.lower()
            for addr, decimals in chain_decimals.items():
                if addr.lower() == addr_lower:
                    return decimals
        except ImportError:
            pass
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
            relevant_event_types = {"BORROW", "REPAY"}
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

        position_id = lending_position_id(
            chain=chain,
            protocol=position_info.protocol or "",
            wallet=wallet,
            asset=asset,
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
