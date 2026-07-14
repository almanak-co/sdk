"""Position health monitoring for lending protocols.

Provides health factor calculations and deleverage trigger detection for any
lending connector publishing the account-state read seam (the Aave family,
Morpho Blue, Silo V2, Euler V2, BENQI) or a multi-collateral market-health
reader (Compound V3), with support for principal-token collateral enrichment
via connector-owned market readers. Dispatch is capability-driven — the
framework names no protocol; connectors opt in through their manifests
(VIB-4851).

Key Classes:
    PositionHealth: Health factor and risk metrics for a lending position
    PTPositionHealth: Extended health data for PT-collateral positions
    DeleverageTrigger: Warning/critical thresholds for automated deleverage
    HealthFactorProvider: Protocol for per-protocol health-factor adapters
    PositionHealthProvider: Reads on-chain position data and computes health

Module-level API:
    get_health_factor(chain, protocol, wallet, market, ...) -> Decimal
        Unified dispatch that returns just the health factor for a lending
        position. Uses each protocol's canonical on-chain source.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.connectors._strategy_base.lending_read_base import (
        LendingAccountState,
        LendingPositionRef,
    )
    from almanak.framework.gateway_client import GatewayClient


# =============================================================================
# Price-source provenance
# =============================================================================
# A health factor is only as trustworthy as the prices it was computed from, so
# the result must STATE where those prices came from — an oracle-defaulted HF
# must never be silently indistinguishable from an override-computed one.

#: Caller-supplied ``collateral_price_usd`` / ``debt_price_usd`` overrides.
PRICE_SOURCE_OVERRIDE = "override"
#: The market's OWN liquidation oracle (exact — the price the protocol
#: liquidates against), with the loan leg's USD conversion from the wired
#: price oracle / stablecoin table.
PRICE_SOURCE_MARKET_ORACLE = "market_oracle"
#: Generic USD price oracle for both legs (approximate — may deviate from the
#: protocol's own liquidation oracle).
PRICE_SOURCE_USD_ORACLE = "usd_oracle"
#: Same-asset market with no override: a unit placeholder price is injected
#: (the price cancels in the HF, so the HF is exact; the USD-denominated value
#: fields are token-denominated).
PRICE_SOURCE_SAME_ASSET_UNIT = "same_asset_unit"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PositionHealth:
    """Health factor and risk metrics for a lending position.

    Attributes:
        health_factor: Ratio of (collateral * LLTV) / debt.
            > 1.0 = healthy, < 1.0 = liquidatable, Infinity = no debt.
        collateral_value_usd: USD value of deposited collateral.
        debt_value_usd: USD value of outstanding debt.
        lltv: Liquidation loan-to-value ratio of the market.
        max_borrow_usd: Additional USD borrowable before liquidation.
        protocol: Protocol name ("morpho_blue", "aave_v3").
        market_id: Protocol-specific market identifier.
        price_source: Provenance of the prices the health computation used —
            one of the ``PRICE_SOURCE_*`` constants, or ``""`` when the
            protocol's valuation is native (Aave-family USD-denominated reads,
            BENQI's on-chain price legs) and no external price was injected.
    """

    health_factor: Decimal
    collateral_value_usd: Decimal
    debt_value_usd: Decimal
    lltv: Decimal
    max_borrow_usd: Decimal = Decimal("0")
    protocol: str = ""
    market_id: str = ""
    price_source: str = ""

    @property
    def is_healthy(self) -> bool:
        """Position is above liquidation threshold."""
        return self.health_factor >= Decimal("1.0")

    @property
    def is_warning(self) -> bool:
        """Position is below the safe threshold (< 1.5) but still healthy."""
        return Decimal("1.0") <= self.health_factor < Decimal("1.5")

    @property
    def is_critical(self) -> bool:
        """Position is very close to liquidation (< 1.1)."""
        return self.health_factor < Decimal("1.1")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "health_factor": str(self.health_factor),
            "collateral_value_usd": str(self.collateral_value_usd),
            "debt_value_usd": str(self.debt_value_usd),
            "lltv": str(self.lltv),
            "max_borrow_usd": str(self.max_borrow_usd),
            "is_healthy": self.is_healthy,
            "is_warning": self.is_warning,
            "is_critical": self.is_critical,
            "protocol": self.protocol,
            "market_id": self.market_id,
            "price_source": self.price_source,
        }


@dataclass
class PTPositionHealth(PositionHealth):
    """Extended health data for PT-collateral positions.

    Adds principal-token metrics: implied APY at liquidation point,
    PT discount, and maturity risk indicators.
    """

    implied_apy: Decimal = Decimal("0")
    pt_discount_pct: Decimal = Decimal("0")
    days_to_maturity: int = 0
    pendle_market: str = ""
    principal_token_market: str = ""

    def __post_init__(self) -> None:
        """Keep the legacy Pendle field and generic PT field in sync."""
        if self.principal_token_market and self.pendle_market:
            if self.principal_token_market != self.pendle_market:
                raise ValueError("principal_token_market and pendle_market must match when both are provided")
        elif self.principal_token_market and not self.pendle_market:
            self.pendle_market = self.principal_token_market
        elif self.pendle_market and not self.principal_token_market:
            self.principal_token_market = self.pendle_market

    @property
    def maturity_risk(self) -> str:
        """Assess maturity-related risk."""
        if self.days_to_maturity < 0:
            return "unknown"
        elif self.days_to_maturity == 0:
            return "expired"
        elif self.days_to_maturity <= 7:
            return "imminent"
        elif self.days_to_maturity <= 30:
            return "near"
        return "safe"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        base = super().to_dict()
        base.update(
            {
                "implied_apy": str(self.implied_apy),
                "pt_discount_pct": str(self.pt_discount_pct),
                "days_to_maturity": self.days_to_maturity,
                "principal_token_market": self.principal_token_market,
                "pendle_market": self.pendle_market,
                "maturity_risk": self.maturity_risk,
            }
        )
        return base


@dataclass
class DeleverageTrigger:
    """Thresholds that determine when to deleverage a position.

    Used by strategies to automate position management:
    - warning_hf: Log a warning and potentially start preparing unwind
    - critical_hf: Trigger immediate deleverage to safe_target_hf
    """

    warning_hf: Decimal = Decimal("1.5")
    critical_hf: Decimal = Decimal("1.2")
    safe_target_hf: Decimal = Decimal("2.0")
    max_leverage: Decimal = Decimal("10")

    def should_deleverage(self, health: PositionHealth) -> bool:
        """Check if health factor is below critical threshold."""
        return health.health_factor < self.critical_hf

    def should_warn(self, health: PositionHealth) -> bool:
        """Check if health factor is in warning zone."""
        return health.health_factor < self.warning_hf


# =============================================================================
# Position Health Provider
# =============================================================================


@dataclass
class _CachedHealth:
    """Internal cache entry for position health data."""

    health: PositionHealth
    block_number: int = 0


class PositionHealthProvider:
    """Reads on-chain position data and computes health factors.

    Supports every lending connector publishing an account-state read spec
    (Aave V3 / Spark, Morpho Blue, Silo V2, Euler V2, BENQI) or a
    multi-collateral market-health reader (Compound V3). For Morpho positions
    with PT collateral, can also compute PT-specific risk metrics.

    Account-state reads route through the connector-owned lending-read seam
    (``read_lending_account_state``); market-health protocols route through the
    connector-owned multi-collateral health read (``read_lending_market_health``,
    VIB-4851 PR-2). Both paths therefore REQUIRE a connected ``gateway_client`` —
    VIB-4851 removed the in-strategy ``Web3(HTTPProvider)`` path for every lending
    protocol. (``rpc_url`` survives only for connector-owned principal-token
    enrichment readers, VIB-4931's territory.)

    Usage:
        provider = PositionHealthProvider(chain="ethereum", gateway_client=gw)
        health = provider.get_health("morpho_blue", market_id, wallet_address)
        print(f"Health factor: {health.health_factor}")
    """

    def __init__(
        self,
        rpc_url: str = "",
        chain: str = "ethereum",
        price_oracle: Any = None,
        gateway_client: "GatewayClient | None" = None,
    ):
        self._rpc_url = rpc_url
        # Canonicalize chain aliases (e.g. "bnb" -> "bsc") via the central
        # resolver, mirroring the intent compiler / execution path. Connector
        # address tables (e.g. AAVE_V3_POOL_ADDRESSES) are keyed on canonical
        # names, so without this an alias would miss the pool lookup and raise
        # "not configured" even on a supported chain. Fall back to the raw
        # value if the resolver is unavailable or the name is unknown (an
        # unknown chain still fails closed at the per-protocol address lookup).
        try:
            from almanak.core.constants import resolve_chain_name

            self._chain = resolve_chain_name(chain)
        except (ValueError, ImportError):
            self._chain = chain
        self._price_oracle = price_oracle
        self._gateway_client = gateway_client
        self._cache: dict[str, _CachedHealth] = {}

    def get_health(
        self,
        protocol: str,
        market_id: str,
        user_address: str,
        collateral_price_usd: Decimal | None = None,
        debt_price_usd: Decimal | None = None,
        ref: "LendingPositionRef | None" = None,
    ) -> PositionHealth:
        """Get health factor for a lending position.

        Args:
            protocol: Any lending protocol publishing the account-state read
                seam or a market-health reader (e.g. "aave_v3", "morpho_blue",
                "compound_v3", "silo_v2", "euler_v2", "benqi", or an alias
                handled by :func:`_normalize_protocol`).
            market_id: Protocol-specific market identifier. For whole-account
                protocols (the Aave family) this is ignored / informational.
                For per-market protocols it scopes the read: Morpho Blue takes
                the bytes32 market id; Compound V3 the Comet market key (e.g.
                "usdc", "weth"); Silo V2 / Euler V2 / BENQI their synthetic
                ``"<col>"`` / ``"<col>/<loan>"`` ids. May be empty when a typed
                ``ref`` is supplied instead (see below).
            user_address: Wallet address holding the position
            collateral_price_usd: Optional override for collateral price on
                cross-asset markets of non-USD-native protocols. When BOTH
                overrides are omitted, the framework defaults to the market's
                own liquidation oracle (exact — connector-declared, e.g. Morpho
                Blue), then to the wired USD price oracle; it fails closed when
                neither source answers. The result's ``price_source`` states
                which source valued the position. Supply both overrides or
                neither — a partial pair is ambiguous and raises.
            debt_price_usd: Optional override for debt token price
            ref: Optional typed :class:`LendingPositionRef` (VIB-5775). Callers
                that hold the position's tokens but NO ``market_id`` — the case
                for synthetic-market protocols (Euler V2, Silo V2) whose intents
                carry none — pass this so the framework can derive the
                ``market_id`` from the connector-declared resolver. Used ONLY
                when ``market_id`` is empty; an explicit ``market_id`` is
                byte-for-byte unaffected (the ref is ignored).

        Returns:
            PositionHealth with computed health factor

        Raises:
            ValueError: If protocol is unsupported
        """
        protocol_lower = _normalize_protocol(protocol)
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        # VIB-5775: synthetic-market protocols (euler_v2, silo_v2, benqi) carry NO
        # market_id on their intents; teardown/valuation supply a typed ref instead.
        # Derive the market_id from the connector-declared resolver ONCE here — before
        # any downstream consumer (``_read_account_state`` AND ``_build_price_oracle_dict``
        # both need it) — so the whole read is scoped correctly. An explicit market_id
        # is untouched (derivation only fires when empty); a ref that cannot resolve
        # leaves market_id empty and the existing empty-id errors fire unchanged.
        if not market_id and ref is not None:
            derived_market_id = LendingReadRegistry.resolve_market_id(ref)
            if derived_market_id:
                market_id = derived_market_id

        # Dispatch on connector-declared capabilities, never on protocol names
        # (VIB-4851). Order matters: a protocol that publishes BOTH a
        # multi-collateral market-health reader and an account-state spec
        # (Compound V3) takes the market-health path — the summed-HF product
        # contract that the single-leg account-state read cannot express.
        if LendingReadRegistry.market_health_reader(protocol_lower) is not None:
            return self._get_market_health(protocol_lower, market_id, user_address)

        if not LendingReadRegistry.supports_account_state(protocol_lower):
            raise ValueError(f"Unsupported protocol for health monitoring: {protocol}")

        price_oracle: dict[str, Decimal] | None = None
        price_source = ""
        if LendingReadRegistry.publishes_market_table(protocol_lower):
            # Per-market protocol (Morpho Blue, Silo V2, Euler V2, BENQI): the
            # caller's market id scopes the read, and the legacy collateral/debt
            # price overrides are translated onto the connector-declared
            # valuation roles via ``_build_price_oracle_dict`` (``None`` for a
            # USD-native per-market protocol that declares no roles).
            price_oracle, price_source = self._build_price_oracle_dict(
                protocol_lower, market_id, collateral_price_usd, debt_price_usd
            )

        # Whole-account protocols (the Aave family — USD-native, no market
        # table) fall through with no oracle dict; ``_read_account_state``
        # drops the informational market id for them.
        state = self._read_account_state(protocol_lower, market_id, user_address, price_oracle=price_oracle)
        return self._to_position_health(state, protocol=protocol_lower, market_id=market_id, price_source=price_source)

    def get_pt_position_health(
        self,
        morpho_market_id: str,
        principal_token_market_address: str | None = None,
        user_address: str = "",
        collateral_price_usd: Decimal | None = None,
        debt_price_usd: Decimal | None = None,
        *,
        principal_token_protocol: str | None = None,
        pendle_market_address: str | None = None,
    ) -> PTPositionHealth:
        """Get extended health data for a PT-collateral position on Morpho.

        Combines Morpho position data with connector-owned principal-token
        market data for comprehensive risk assessment.

        Args:
            morpho_market_id: Morpho Blue market ID
            principal_token_market_address: Principal-token market address for the PT
            user_address: Wallet address
            collateral_price_usd: Override for PT collateral price
            debt_price_usd: Override for debt token price
            principal_token_protocol: Optional connector protocol key. When
                omitted, the sole registered principal-token reader is used for
                backward compatibility.
            pendle_market_address: Deprecated alias for
                ``principal_token_market_address``.

        Returns:
            PTPositionHealth with Morpho + principal-token risk metrics
        """
        pt_market_address = principal_token_market_address or pendle_market_address or ""
        if not pt_market_address:
            raise ValueError("principal_token_market_address is required for PT position health")
        if not user_address:
            raise ValueError("user_address is required for PT position health")
        principal_token_registry: Any | None = None
        if principal_token_protocol:
            from almanak.connectors._strategy_principal_token_market_reader_registry import (
                PRINCIPAL_TOKEN_MARKET_READ_REGISTRY,
            )

            principal_token_registry = PRINCIPAL_TOKEN_MARKET_READ_REGISTRY
            if principal_token_registry.lookup(principal_token_protocol) is None:
                raise ValueError(f"unknown principal_token_protocol {principal_token_protocol!r}")

        # Get base Morpho health via the shared seam-backed Morpho path
        # (VIB-4851). ``get_health`` owns the price-override translation +
        # ``read_lending_account_state`` round-trip; the connector-owned
        # principal-token enrichment below (VIB-4931's territory) is layered on top.
        base_health = self.get_health(
            "morpho_blue",
            morpho_market_id,
            user_address,
            collateral_price_usd=collateral_price_usd,
            debt_price_usd=debt_price_usd,
        )

        # Get principal-token-specific data from the connector-owned reader.
        implied_apy = Decimal("0")
        pt_discount_pct = Decimal("0")
        days_to_maturity = 0

        try:
            if principal_token_registry is None:
                from almanak.connectors._strategy_principal_token_market_reader_registry import (
                    PRINCIPAL_TOKEN_MARKET_READ_REGISTRY,
                )

                principal_token_registry = PRINCIPAL_TOKEN_MARKET_READ_REGISTRY

            reader_kwargs: dict[str, Any] = {"chain": self._chain}
            if self._gateway_client is not None:
                reader_kwargs["gateway_client"] = self._gateway_client
            else:
                reader_kwargs["rpc_url"] = self._rpc_url

            if principal_token_protocol:
                reader = principal_token_registry.build_reader(
                    principal_token_protocol,
                    **reader_kwargs,
                )
            else:
                reader = principal_token_registry.build_default_reader(**reader_kwargs)

            implied_apy = reader.get_implied_apy(pt_market_address)

            # Check if market is expired
            if reader.is_market_expired(pt_market_address):
                days_to_maturity = 0
            else:
                # Estimate days from PT discount (PT trades below 1:1 before maturity)
                pt_rate = reader.get_pt_to_asset_rate(pt_market_address)
                if pt_rate < Decimal("1"):
                    pt_discount_pct = (Decimal("1") - pt_rate) * Decimal("100")
                    # Rough estimate: if APY is known, days ~ discount / (APY / 365)
                    if implied_apy > 0:
                        daily_rate = implied_apy / Decimal("365")
                        if daily_rate > 0:
                            days_to_maturity = int(pt_discount_pct / Decimal("100") / daily_rate)

        except Exception as e:
            logger.warning("Failed to get principal-token data for PT health: %s", e)

        return PTPositionHealth(
            health_factor=base_health.health_factor,
            collateral_value_usd=base_health.collateral_value_usd,
            debt_value_usd=base_health.debt_value_usd,
            lltv=base_health.lltv,
            max_borrow_usd=base_health.max_borrow_usd,
            protocol=base_health.protocol,
            market_id=base_health.market_id,
            price_source=base_health.price_source,
            implied_apy=implied_apy,
            pt_discount_pct=pt_discount_pct,
            days_to_maturity=days_to_maturity,
            principal_token_market=pt_market_address,
        )

    def _read_account_state(
        self,
        protocol: str,
        market_id: str,
        user_address: str,
        price_oracle: dict | None = None,
    ) -> "LendingAccountState":
        """Read aggregate account state via the connector-owned lending-read seam.

        Routes Aave/Spark and Morpho health reads through
        :func:`~almanak.framework.accounting.lending_reads.read_lending_account_state`,
        the single generic reader that resolves the read target from the same
        ``addresses.py`` / ``AddressRegistry`` the intent path uses, owns the one
        gateway round-trip, and fails closed (returns ``None``) on any missing
        input.

        VIB-4851: the legacy ``Web3(HTTPProvider(rpc_url))`` fallback -- a
        gateway-boundary violation -- is gone for Aave/Morpho. A gateway client is
        now required; a missing or disconnected one raises a clear ``ValueError``
        (mirroring the pre-refactor connected-check) so the failure surfaces
        instead of fabricating a false-safe health factor.
        """
        from almanak.framework.accounting.lending_reads import read_lending_account_state

        if self._gateway_client is None:
            raise ValueError(
                f"GatewayClient is required to read {protocol} health on {self._chain}; none was provided."
            )
        if not self._gateway_client.is_connected:
            raise ValueError(f"GatewayClient is not connected; cannot fetch {protocol} health on {self._chain}.")

        # Per-market protocols (those publishing a market table: Morpho Blue,
        # Silo V2, Euler V2, BENQI) pass the caller's market id straight
        # through; whole-account protocols (the Aave family) carry none.
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        seam_market_id = market_id if LendingReadRegistry.publishes_market_table(protocol) else None

        # Thread the market's collateral token so the generic reader prices + injects
        # the collateral leg — mirroring the accounting path, which calls
        # ``read_lending_account_state`` with ``collateral_token=`` from the spec's
        # query inputs (``lending_accounting.build_lending_accounting_event``). Without
        # it a SINGLE-LEG (supply-only) synthetic euler/silo market reads no collateral
        # and comes back unmeasured → the WITHDRAW is refused (VIB-5775). Additive for
        # two-leg markets: the collateral symbol is already priced via valuation roles,
        # so this only resolves its address (no new failure surface). ``None`` for
        # whole-account protocols (Aave — no market table) and for BENQI (its
        # whole-account params name no single ``collateral_token``).
        collateral_token: str | None = None
        if seam_market_id:
            market_params = LendingReadRegistry.market_params(protocol, self._chain, seam_market_id)
            if market_params:
                catalogue_collateral = market_params.get("collateral_token")
                if isinstance(catalogue_collateral, str) and catalogue_collateral:
                    collateral_token = catalogue_collateral

        state = read_lending_account_state(
            protocol=protocol,
            chain=self._chain,
            wallet_address=user_address,
            market_id=seam_market_id,
            gateway_client=self._gateway_client,
            price_oracle=price_oracle,
            collateral_token=collateral_token,
        )
        if state is None:
            # Empty != Zero: a failed read must surface, never become a fabricated
            # healthy/zero HF that would mask a liquidation-risk position.
            raise ValueError(
                f"Failed to read {protocol} account state for market "
                f"{(market_id or '')[:10]}... on {self._chain} (read returned no data)."
            )
        return state

    def _to_position_health(
        self,
        state: "LendingAccountState",
        protocol: str,
        market_id: str,
        price_source: str = "",
    ) -> PositionHealth:
        """Adapt a seam :class:`LendingAccountState` to a :class:`PositionHealth`.

        Bridges the connector reducer's field shape onto the public health
        contract, preserving two behaviours of the pre-refactor path:

        * **No-debt -> Infinity.** The public contract documents ``Infinity`` for
          a position with no debt. Surface it ONLY when ``debt_usd == 0`` -- a
          *positive* debt whose HF the reducer capped at its 999999 sentinel (a
          tiny borrow against large collateral) stays finite so risk handling is
          not skipped.
        * **Empty != Zero / raise-on-failure.** A ``None`` state, or a ``None`` HF
          with positive debt, RAISES rather than fabricating a healthy/zero HF --
          matching the old "raise on failure" contract so a failed read cannot
          read as false-safe.
        """
        if state is None:
            raise ValueError(
                f"Cannot compute {protocol} health for market {(market_id or '')[:10]}...: account state is unavailable."
            )

        collateral_value = state.collateral_usd if state.collateral_usd is not None else Decimal("0")
        debt_value = state.debt_usd if state.debt_usd is not None else Decimal("0")

        # Map to Infinity ONLY for a genuine no-debt position. ``debt_value == 0``
        # is the authoritative signal: the reducers also emit the 999999 sentinel
        # for no debt, but a *positive* debt whose HF was merely capped at that
        # sentinel (a tiny dust borrow against large collateral) MUST stay finite,
        # or strategies would skip risk/deleverage handling on a still-open debt.
        no_debt = debt_value == 0
        hf = state.health_factor
        if no_debt:
            health_factor: Decimal = Decimal("Infinity")
        elif hf is None:
            # Positive debt but no measured HF => unmeasured, not infinite.
            # Fail closed rather than report a false-safe Infinity.
            raise ValueError(
                f"{protocol} health factor unavailable for market {(market_id or '')[:10]}... "
                f"with non-zero debt; refusing to fabricate a healthy value."
            )
        else:
            # Positive debt: pass the measured HF through unchanged (including a
            # value capped at the reducer's 999999 sentinel).
            health_factor = hf

        # lltv: the Aave family reports a weighted current liquidation threshold in
        # bps (USD-native); Morpho/Compound report an lltv fraction directly. Branch
        # on the measured field, not a protocol name (self-containment).
        if state.liquidation_threshold_bps is not None:
            lltv = Decimal(state.liquidation_threshold_bps) / Decimal("10000")
        else:
            lltv = state.lltv if state.lltv is not None else Decimal("0")

        max_borrow = max(collateral_value * lltv - debt_value, Decimal("0"))

        return PositionHealth(
            health_factor=health_factor,
            collateral_value_usd=collateral_value,
            debt_value_usd=debt_value,
            lltv=lltv,
            max_borrow_usd=max_borrow,
            protocol=protocol,
            market_id=market_id,
            price_source=price_source,
        )

    def _build_price_oracle_dict(
        self,
        protocol: str,
        market_id: str,
        collateral_price_usd: Decimal | None,
        debt_price_usd: Decimal | None,
    ) -> tuple[dict[str, Decimal] | None, str]:
        """Resolve the seam's oracle dict from overrides, else oracle defaults.

        Non-USD-native per-market protocols (Morpho Blue, Silo V2, Euler V2)
        are valued from a ``{token_symbol: USD price}`` map. This method maps
        the legacy ``collateral_price_usd`` / ``debt_price_usd`` Decimal
        overrides onto that shape, keyed by the symbols the connector's
        valuation roles name (``collateral_token`` / ``loan_token`` — the
        ``AccountStateQuery`` field-name convention every spec declares).
        Returns ``(prices, price_source)`` — the injected map plus the
        ``PRICE_SOURCE_*`` provenance the result must carry.

        * **Same-asset market** (collateral symbol == loan symbol): one symbol, one
          price -- use whichever override is supplied (else ``Decimal("1")``). The
          HF is price-independent here (the price cancels), so a single consistent
          key is correct and avoids a duplicate-key override drop.
        * **Cross-asset market, both overrides supplied**: the overrides win
          unconditionally (absolute precedence) — no oracle is consulted.
        * **Cross-asset market, no overrides**: default to the
          market's OWN liquidation oracle when the connector declares one
          (exact — the price the protocol liquidates against), else to the
          wired USD price oracle for both legs; RAISE ``ValueError`` (message
          contains ``"Price overrides required"``) when neither source answers
          -- never default to 1:1 across differing assets.
        * **Cross-asset market, exactly one override**: RAISE — a partial pair
          is ambiguous, and silently completing it with a different-provenance
          default could produce an HF the caller never intended.
        * **Off-catalogue market** (``market_params`` is ``None``): fail closed
          consistently with a ``ValueError`` -- we cannot name the legs to value.

        A USD-native per-market protocol that declares no valuation roles
        (BENQI — its qiToken reads price legs on-chain) returns ``(None, "")``:
        there is nothing to inject.
        """
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        if not market_id:
            raise ValueError(
                f"market_id is required to value a {protocol} position on {self._chain}; none was provided."
            )

        params = LendingReadRegistry.market_params(protocol, self._chain, market_id)
        if not params:
            raise ValueError(
                f"{protocol} market {market_id} not found on {self._chain}; cannot resolve "
                f"its collateral/loan tokens to value the position."
            )

        # Single-leg (supply-only) synthetic market (VIB-5775): a collateral leg with
        # NO loan leg — the euler_v2 / silo_v2 ``"<collateral>"`` catalogue entries: a
        # pure supply position (no controller vault, no debt). The two-leg cross-asset
        # logic below fails closed here (the declared ``loan_token`` role resolves to
        # None ⇒ ``valuation_roles`` returns () ⇒ the ``declares_valuation_roles``
        # branch RAISES "has no collateral/loan token symbols"), but a one-leg market
        # is legitimately valued from a SINGLE collateral price — its debt is a
        # measured zero. Detected from the params SHAPE (general — not euler-specific,
        # also covers silo_v2 and any synthetic single-leg supply), so the account-state
        # read runs and returns measured collateral + ``Decimal("0")`` debt instead of
        # this method aborting the whole health read. The HF is debt-free (the price
        # cancels), so a missing override uses the unit placeholder exactly like the
        # same-asset path. BENQI (no ``collateral_token`` key in its whole-account
        # params) and every two-leg market (``loan_token`` present) skip this branch.
        collateral_only_symbol = params.get("collateral_token")
        if params.get("loan_token") is None and isinstance(collateral_only_symbol, str) and collateral_only_symbol:
            if collateral_price_usd is not None:
                return {collateral_only_symbol: collateral_price_usd}, PRICE_SOURCE_OVERRIDE
            # DENOMINATION CAVEAT (PR #3277 audit — codex/CodeRabbit/pr-auditor): in the
            # NO-override path (reached only when the caller's own oracle read returned no
            # price, e.g. a transient price-feed outage) the resulting
            # ``collateral_value_usd`` is token-DENOMINATED, not USD, for a non-USD-pegged
            # collateral. This is safe for the two proven money-consumers: the teardown
            # guard and ``generate_lending_unwind`` both PASS a real override on the happy
            # path (so they never reach here), and the only consumer of this degraded value
            # is a conservative ``> _DUST_USD`` gate before a risk-reducing ``withdraw_all``
            # — it never sizes a real trade (two-leg sizing requires both real prices and
            # raises otherwise). A USD-accurate no-override valuation for non-stable
            # single-leg collateral is tracked as a follow-up (a real USD oracle must be
            # threaded into this pricing dict; not available in-method today). The unit
            # placeholder is correct for USD-pegged collateral (the tested/proven token).
            return {collateral_only_symbol: Decimal("1")}, PRICE_SOURCE_SAME_ASSET_UNIT

        roles = dict(LendingReadRegistry.valuation_roles(protocol, self._chain, market_id))
        if not roles:
            if LendingReadRegistry.declares_valuation_roles(protocol):
                # Declared roles that resolve to no symbols = a malformed
                # catalogue entry; fail closed rather than read unpriced legs.
                raise ValueError(
                    f"{protocol} market {market_id} on {self._chain} has no collateral/loan "
                    f"token symbols; cannot value the position."
                )
            return None, ""

        collateral_symbol = roles.get("collateral_token")
        loan_symbol = roles.get("loan_token")
        if not collateral_symbol or not loan_symbol:
            raise ValueError(
                f"{protocol} declares valuation roles {sorted(roles)} that do not map onto "
                f"the collateral/debt price-override contract; cannot value the position."
            )

        same_asset = collateral_symbol.lower() == loan_symbol.lower()

        # Key by symbol; the seam's ``_resolve_oracle_price`` is case-insensitive.
        if same_asset:
            # Collateral and loan are the same token: one symbol, one price. The HF
            # is price-independent here (the price cancels), so build a single-key
            # dict -- a two-key ``{loan, collateral}`` literal would collapse to one
            # entry and silently drop a lone ``debt_price_usd`` override.
            if collateral_price_usd is not None:
                return {collateral_symbol: collateral_price_usd}, PRICE_SOURCE_OVERRIDE
            if debt_price_usd is not None:
                return {collateral_symbol: debt_price_usd}, PRICE_SOURCE_OVERRIDE
            return {collateral_symbol: Decimal("1")}, PRICE_SOURCE_SAME_ASSET_UNIT

        # Cross-asset with both overrides: caller-supplied prices win
        # unconditionally (absolute precedence — no oracle is consulted).
        if collateral_price_usd is not None and debt_price_usd is not None:
            return {collateral_symbol: collateral_price_usd, loan_symbol: debt_price_usd}, PRICE_SOURCE_OVERRIDE

        # Cross-asset with a PARTIAL override: ambiguous — the caller clearly
        # intended to control pricing but named only one leg. Completing the
        # pair with a different-provenance default could silently produce an
        # HF they never intended, so fail closed (pre-existing contract preserved).
        if collateral_price_usd is not None or debt_price_usd is not None:
            raise ValueError(
                f"Price overrides required for cross-asset {protocol} market {market_id}: "
                f"exactly one of collateral_price_usd / debt_price_usd was provided. "
                f"Pass both, or neither (to value from the market's own oracle)."
            )

        # Cross-asset with NO overrides: default to the market's own
        # liquidation oracle, else the wired USD oracle; fail closed when neither
        # answers -- never default to 1:1 across differing assets.
        defaults = self._default_cross_asset_prices(protocol, market_id, collateral_symbol, loan_symbol)
        if defaults is not None:
            prices, source = defaults
            logger.info(
                "%s market %s on %s: no price overrides provided; valuing position from %s (%s=%s USD, %s=%s USD)",
                protocol,
                market_id,
                self._chain,
                source,
                collateral_symbol,
                prices[collateral_symbol],
                loan_symbol,
                prices[loan_symbol],
            )
            return prices, source

        raise ValueError(
            f"Price overrides required for cross-asset {protocol} market {market_id}. "
            f"Collateral and debt tokens differ -- cannot default to 1:1, the market's "
            f"own oracle could not be read, and no USD price source answered for both "
            f"legs. Pass collateral_price_usd / debt_price_usd, or wire a price oracle."
        )

    def _default_cross_asset_prices(
        self,
        protocol: str,
        market_id: str,
        collateral_symbol: str,
        loan_symbol: str,
    ) -> tuple[dict[str, Decimal], str] | None:
        """Build the default cross-asset price map when no overrides were given.

        Source order:

        1. **The market's OWN liquidation oracle** (connector-declared via
           ``MarketOraclePriceSpec``; gateway-routed read). Exact by
           construction — it returns the collateral price denominated in the
           loan token, THE price the protocol computes liquidation against, so
           the resulting HF matches the on-chain health check. The loan leg's
           USD conversion comes from the wired price oracle / stablecoin table;
           it scales both value fields identically, so the HF stays exact even
           if that USD quote drifts (it cancels in the ratio).
        2. **The wired USD price oracle for both legs** — approximate (may
           deviate from the protocol's own oracle), used only when the market
           oracle cannot be read.

        Returns ``(prices, source)`` or ``None`` when neither source answers
        (the caller then fails closed — Empty ≠ Zero, never 1:1 across
        differing assets).
        """
        from almanak.framework.accounting.lending_reads import read_market_oracle_price

        if self._gateway_client is not None and getattr(self._gateway_client, "is_connected", False):
            collateral_in_loan = read_market_oracle_price(
                protocol=protocol,
                chain=self._chain,
                market_id=market_id,
                gateway_client=self._gateway_client,
            )
            if collateral_in_loan is not None and collateral_in_loan > 0:
                loan_usd = self._try_resolve_usd_price(loan_symbol)
                if loan_usd is not None and loan_usd > 0:
                    return (
                        {collateral_symbol: collateral_in_loan * loan_usd, loan_symbol: loan_usd},
                        PRICE_SOURCE_MARKET_ORACLE,
                    )
                logger.warning(
                    "%s market %s on %s: market oracle priced %s at %s %s, but no USD "
                    "price source answered for %s; falling back to the USD oracle for both legs.",
                    protocol,
                    market_id,
                    self._chain,
                    collateral_symbol,
                    collateral_in_loan,
                    loan_symbol,
                    loan_symbol,
                )

        collateral_usd = self._try_resolve_usd_price(collateral_symbol)
        loan_usd = self._try_resolve_usd_price(loan_symbol)
        if collateral_usd is not None and collateral_usd > 0 and loan_usd is not None and loan_usd > 0:
            return {collateral_symbol: collateral_usd, loan_symbol: loan_usd}, PRICE_SOURCE_USD_ORACLE
        return None

    def _try_resolve_usd_price(self, symbol: str) -> Decimal | None:
        """Best-effort USD price via :meth:`_resolve_base_price` (``None`` on failure).

        The default-pricing path needs "answered / did not answer" rather than
        the raising contract ``_resolve_base_price`` keeps for the Compound V3
        market-health read — a missing quote here falls through to the next
        source or to the fail-closed override error.
        """
        try:
            return self._resolve_base_price(symbol)
        except ValueError:
            return None

    # Market-health base tokens that are USD-pegged stablecoins. These are the
    # *only* symbols for which it is safe to assume price == $1 when no external
    # price oracle is provided. Anything else (WETH, AERO, WBTC, etc.) MUST have
    # an explicit ``price_oracle`` or the HF computation fails closed.
    _STABLE_BASE_SYMBOLS: frozenset[str] = frozenset(
        {"USDC", "USDT", "USDS", "USDC.E", "DAI", "FRAX", "LUSD", "USDBC", "SUSDS"}
    )

    def _resolve_base_decimals(self, symbol: str, address: str) -> int:
        """Resolve base-token decimals via the unified TokenResolver.

        Raises on failure. NEVER guesses -- a wrong decimals value silently
        mis-scales debt by orders of magnitude (e.g. WETH=18 vs USDC=6 is a
        1e12 miscalculation). Per CLAUDE.md: "NEVER default to 18 decimals
        - always raise TokenNotFoundError if decimals unknown." Used by the
        market-health read path (Compound V3 today).
        """
        from almanak.framework.data.tokens import TokenNotFoundError, get_token_resolver

        try:
            resolver = get_token_resolver()
            token = resolver.resolve(symbol, self._chain)
            return int(token.decimals)
        except TokenNotFoundError:
            raise
        except Exception as e:
            raise TokenNotFoundError(
                token=symbol,
                chain=self._chain,
                reason=(
                    f"Could not resolve decimals for market-health base token "
                    f"{symbol!r} ({address}) on {self._chain}: {e}. "
                    f"Refusing to guess -- a wrong decimals value silently mis-scales debt."
                ),
            ) from e

    def _resolve_base_price(self, symbol: str) -> Decimal:
        """Resolve a token's USD price from the wired oracle / stablecoin table.

        Used by the Compound V3 market-health read (base/borrow token) and, via
        the non-raising :meth:`_try_resolve_usd_price` wrapper, by the
        cross-asset default-pricing path.

        Price-source protocol, in order:
          1. ``price_oracle`` supports ``.get_aggregated_price(symbol, quote)``
             (async PriceOracle Protocol) -> await it.
          2. ``price_oracle`` is a plain callable ``symbol -> number`` -> call it.
          3. ``price_oracle`` is None AND ``symbol`` is a known USD stablecoin
             -> return ``Decimal("1")``.
          4. Otherwise -> raise. Silent ``base_price = 1`` on non-stable bases
             would inflate reported HF by orders of magnitude.
        """
        oracle = self._price_oracle
        if oracle is not None:
            # Case 1: PriceOracle Protocol (async).
            get_price = getattr(oracle, "get_aggregated_price", None)
            if get_price is not None and callable(get_price):
                try:
                    coro = get_price(symbol, "USD")
                    # May be a coroutine -> run to completion.
                    import asyncio
                    import inspect

                    if inspect.iscoroutine(coro):
                        try:
                            # Detect a running loop; if we're inside one, dispatch to a thread.
                            asyncio.get_running_loop()
                            import concurrent.futures

                            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                                result = ex.submit(asyncio.run, coro).result()
                        except RuntimeError:
                            # No running loop -> safe to use asyncio.run directly.
                            result = asyncio.run(coro)
                    else:
                        result = coro
                    price = getattr(result, "price", result)
                    return Decimal(str(price))
                except Exception as e:
                    logger.warning(f"Market-health PriceOracle({symbol}) failed: {e}")
            # Case 2: plain callable.
            elif callable(oracle):
                try:
                    return Decimal(str(oracle(symbol)))
                except Exception as e:
                    logger.warning(f"Market-health price_oracle({symbol}) failed: {e}")

        # Case 3: stablecoin 1:1 fallback.
        if symbol.upper() in self._STABLE_BASE_SYMBOLS:
            return Decimal("1")

        # Case 4: fail closed -- refuse to silently mis-price non-stable bases.
        raise ValueError(
            f"Market-health base token {symbol!r} is not a recognized USD stablecoin "
            f"and no working price_oracle was provided. Pass an async PriceOracle "
            f"(with .get_aggregated_price) or a callable(symbol)->Decimal to "
            f"PositionHealthProvider / MarketSnapshot.position_health() to avoid "
            f"inflating reported health factor."
        )

    def _get_market_health(
        self,
        protocol: str,
        market_id: str,
        user_address: str,
    ) -> PositionHealth:
        """Compute a multi-collateral market health factor via the connector seam.

        VIB-4851 PR-2: routes the read through the connector-owned, gateway-routed
        :func:`~almanak.framework.accounting.lending_reads.read_lending_market_health`
        -> the manifest-declared market-health reader (Compound V3 today). That
        reader preserves the product-owner-chosen *summed* health factor
        ``HF = Σ_over_held_collaterals(value_usd × LCF) / borrow_value_usd`` exactly —
        the per-collateral price / scale / liquidation factor are read ON-CHAIN
        (``getAssetInfoByAddress`` / ``getPrice``), and only the base/borrow token
        price + decimals are resolved here (via ``_resolve_base_price`` /
        ``_resolve_base_decimals``, injected into the connector read). No private
        Comet-address copy, no inline Comet ABI, no in-strategy ``Web3(HTTPProvider)``.

        A connected ``gateway_client`` is REQUIRED (mirroring ``_read_account_state``);
        a missing / disconnected one, an unknown chain/market, or a failed read RAISES
        rather than fabricating a false-safe health factor (Empty ≠ Zero).

        Args:
            protocol: Canonical protocol key publishing a market-health reader
                (e.g. ``"compound_v3"``).
            market_id: Per-market key (e.g. "usdc", "weth").
            user_address: Wallet to inspect.
        """
        from almanak.framework.accounting.lending_reads import read_lending_market_health

        if self._gateway_client is None:
            raise ValueError(
                f"GatewayClient is required to read {protocol} health on {self._chain}; none was provided."
            )
        if not self._gateway_client.is_connected:
            raise ValueError(f"GatewayClient is not connected; cannot fetch {protocol} health on {self._chain}.")

        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        # Disambiguate the two fail-closed cases the legacy path raised distinctly
        # (preserved for the byte-equivalence contract): unknown chain vs unknown
        # market. Both lookups are owned by the registry's connector-backed market
        # table — no private address copy is imported here. The market-scoped
        # ``position_manager_address`` is the chain-level existence signal (truthy
        # when the chain has ANY published market for the protocol).
        if LendingReadRegistry.market_health_inputs(protocol, self._chain, market_id) is None:
            if not LendingReadRegistry.position_manager_address(protocol, self._chain):
                raise ValueError(f"{protocol} not configured for chain: {self._chain}")
            raise ValueError(f"{protocol} market '{market_id}' not found on {self._chain}.")

        state = read_lending_market_health(
            protocol=protocol,
            chain=self._chain,
            wallet_address=user_address,
            market_id=market_id,
            gateway_client=self._gateway_client,
            resolve_base_price=self._resolve_base_price,
            resolve_base_decimals=self._resolve_base_decimals,
        )
        if state is None:
            # Empty != Zero: a failed read must surface, never become a fabricated
            # healthy/zero HF that would mask a liquidation-risk position.
            raise ValueError(
                f"Failed to read {protocol} health for market '{market_id}' on {self._chain} (read returned no data)."
            )
        return self._to_position_health(state, protocol=protocol, market_id=market_id)


# =============================================================================
# Unified Health Factor API
# =============================================================================


def _normalize_protocol(protocol: str) -> str:
    """Normalize protocol aliases via the manifest-declared lending alias map.

    Delegates to :meth:`LendingReadRegistry.normalize_protocol` (whitespace /
    case / hyphen folding + the lending aliases each connector declares on its
    ``LendingReadDecl``), so this module owns no alias table of its own
    (VIB-4851 B3). Unknown spellings pass through in folded form and fail
    closed at the capability dispatch.
    """
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    return LendingReadRegistry.normalize_protocol(protocol)


@runtime_checkable
class HealthFactorProvider(Protocol):
    """Protocol for per-protocol health-factor adapters.

    An implementation returns the canonical on-chain health factor for a
    user's position on a specific market. The returned ``Decimal`` follows
    the SDK convention:

        * ``>= 1.0``  = healthy
        * ``< 1.0``   = liquidatable
        * ``Infinity`` = no outstanding debt

    Implementations MUST use each protocol's canonical source (no invented
    formulas). See ``PositionHealthProvider`` for the built-in adapters.
    """

    def get_health_factor(self, wallet: str, market: str) -> Decimal:  # noqa: D401 - Protocol
        """Return the live health factor for ``wallet`` on ``market``."""
        ...


class _PositionHealthProviderAdapter:
    """Wraps :class:`PositionHealthProvider` so it implements the Protocol.

    Bound to a specific protocol so ``get_health_factor(wallet, market)`` is
    unambiguous at call-sites.
    """

    def __init__(self, inner: PositionHealthProvider, protocol: str):
        self._inner = inner
        self._protocol = _normalize_protocol(protocol)

    def get_health_factor(self, wallet: str, market: str) -> Decimal:
        health = self._inner.get_health(
            protocol=self._protocol,
            market_id=market,
            user_address=wallet,
        )
        return health.health_factor


def get_health_factor(
    chain: str,
    protocol: str,
    wallet: str,
    market: str,
    *,
    rpc_url: str = "",
    gateway_client: "GatewayClient | None" = None,
    price_oracle: Any = None,
) -> Decimal:
    """Return the health factor for a lending position.

    Unified dispatch across Aave V3, Morpho Blue, and Compound V3. Each
    protocol uses its canonical on-chain source:

        * **Aave V3**: ``Pool.getUserAccountData().healthFactor``
        * **Morpho Blue**: ``(collateral * LLTV) / debt`` from on-chain position
        * **Compound V3**: ``liquidation_threshold_usd / borrow_value_usd``

    Args:
        chain: Chain name (e.g. "ethereum", "arbitrum").
        protocol: Lending protocol name ("aave_v3", "morpho_blue",
            "compound_v3", or any alias handled by
            :func:`_normalize_protocol`).
        wallet: Wallet address to inspect.
        market: Protocol-specific market identifier. For Aave V3 this is
            informational. For Morpho Blue this is the bytes32 market id.
            For Compound V3 this is the Comet market key (e.g. "usdc").
        rpc_url: HTTP RPC endpoint (when not using gateway).
        gateway_client: Optional gateway client (preferred in production).
        price_oracle: Optional callable ``symbol -> Decimal`` (or async
            PriceOracle) used by the Compound V3 path for base-token pricing
            and by the cross-asset default-pricing path for USD
            conversion when no per-call overrides are supplied.

    Returns:
        ``Decimal`` health factor (``Infinity`` if no debt).

    Raises:
        ValueError: If the protocol is unsupported.
    """
    protocol_n = _normalize_protocol(protocol)
    inner = PositionHealthProvider(
        rpc_url=rpc_url,
        chain=chain,
        price_oracle=price_oracle,
        gateway_client=gateway_client,
    )
    adapter = _PositionHealthProviderAdapter(inner, protocol_n)
    return adapter.get_health_factor(wallet, market)


__all__ = [
    "PRICE_SOURCE_MARKET_ORACLE",
    "PRICE_SOURCE_OVERRIDE",
    "PRICE_SOURCE_SAME_ASSET_UNIT",
    "PRICE_SOURCE_USD_ORACLE",
    "DeleverageTrigger",
    "HealthFactorProvider",
    "PTPositionHealth",
    "PositionHealth",
    "PositionHealthProvider",
    "get_health_factor",
]
