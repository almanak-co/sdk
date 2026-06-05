"""Position health monitoring for lending protocols.

Provides health factor calculations and deleverage trigger detection
for Aave V3, Morpho Blue and Compound V3 positions, with support for
principal-token collateral enrichment via connector-owned market readers.

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
    from almanak.connectors._strategy_base.lending_read_base import LendingAccountState
    from almanak.framework.gateway_client import GatewayClient


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
    """

    health_factor: Decimal
    collateral_value_usd: Decimal
    debt_value_usd: Decimal
    lltv: Decimal
    max_borrow_usd: Decimal = Decimal("0")
    protocol: str = ""
    market_id: str = ""

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

    Supports Aave V3 / Spark, Morpho Blue, and Compound V3. For Morpho positions
    with PT collateral, can also compute PT-specific risk metrics.

    Aave/Spark and Morpho reads route through the connector-owned lending-read
    seam (``read_lending_account_state``); Compound V3 routes through the
    connector-owned multi-collateral health read (``read_lending_market_health``,
    VIB-4851 PR-2). All three therefore REQUIRE a connected ``gateway_client`` —
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
    ) -> PositionHealth:
        """Get health factor for a lending position.

        Args:
            protocol: "morpho_blue", "aave_v3", or "compound_v3"
            market_id: Protocol-specific market identifier. For Aave V3 this
                is ignored / informational (one pool per chain). For Morpho
                Blue this is the bytes32 market id. For Compound V3 this
                is the market key (e.g. "usdc", "weth") used to look up the
                Comet contract.
            user_address: Wallet address holding the position
            collateral_price_usd: Optional override for collateral price
            debt_price_usd: Optional override for debt token price

        Returns:
            PositionHealth with computed health factor

        Raises:
            ValueError: If protocol is unsupported
        """
        protocol_lower = _normalize_protocol(protocol)
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        if protocol_lower == "morpho_blue":
            # VIB-4851: Morpho health reads route through the seam. The legacy
            # collateral/debt price overrides are translated into the seam's
            # ``{symbol: price}`` oracle dict via ``_build_price_oracle_dict``.
            price_oracle = self._build_price_oracle_dict(market_id, collateral_price_usd, debt_price_usd)
            state = self._read_account_state(protocol_lower, market_id, user_address, price_oracle=price_oracle)
            return self._to_position_health(state, protocol="morpho_blue", market_id=market_id)
        elif protocol_lower == "compound_v3":
            return self._get_compound_health(market_id, user_address)
        elif LendingReadRegistry.supports_account_state(protocol_lower):
            # VIB-4851: the Aave V3 family (Aave V3, Spark, and any future fork) is
            # USD-native + whole-account. Route every account-state-capable protocol
            # that is NOT Morpho/Compound through the shared seam, resolved from the
            # registry so adding an Aave fork needs no new protocol name here.
            state = self._read_account_state(protocol_lower, market_id, user_address)
            return self._to_position_health(state, protocol=protocol_lower, market_id=market_id)
        else:
            raise ValueError(f"Unsupported protocol for health monitoring: {protocol}")

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

        # Whole-account protocols (the Aave family) carry no market id; per-market
        # protocols (Morpho) pass the bytes32 market id straight through.
        seam_market_id = market_id if protocol == "morpho_blue" else None

        state = read_lending_account_state(
            protocol=protocol,
            chain=self._chain,
            wallet_address=user_address,
            market_id=seam_market_id,
            gateway_client=self._gateway_client,
            price_oracle=price_oracle,
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
        )

    def _build_price_oracle_dict(
        self,
        market_id: str,
        collateral_price_usd: Decimal | None,
        debt_price_usd: Decimal | None,
    ) -> dict[str, Decimal] | None:
        """Translate legacy Morpho price overrides into the seam's oracle dict.

        The seam values Morpho positions from a ``{token_symbol: USD price}`` map
        (Morpho is not USD-native). This method maps the legacy
        ``collateral_price_usd`` / ``debt_price_usd`` Decimal overrides onto that
        shape, keyed by the market's collateral/loan token *symbols* from
        :meth:`LendingReadRegistry.market_params`, preserving the pre-refactor
        semantics exactly:

        * **Same-asset market** (collateral symbol == loan symbol): one symbol, one
          price -- use whichever override is supplied (else ``Decimal("1")``). The
          HF is price-independent here (the price cancels), so a single consistent
          key is correct and avoids a duplicate-key override drop.
        * **Cross-asset market** with a missing override: RAISE ``ValueError``
          (message contains ``"Price overrides required"``) -- never default to
          1:1 across differing assets.
        * **Off-catalogue market** (``market_params`` is ``None``): fail closed
          consistently with a ``ValueError`` -- we cannot name the legs to value.
        """
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        if not market_id:
            raise ValueError(
                f"market_id is required to value a Morpho Blue position on {self._chain}; none was provided."
            )

        params = LendingReadRegistry.market_params("morpho_blue", self._chain, market_id)
        if not params:
            raise ValueError(
                f"Morpho market {market_id} not found on {self._chain}; cannot resolve "
                f"its collateral/loan tokens to value the position."
            )

        collateral_symbol = params.get("collateral_token")
        loan_symbol = params.get("loan_token")
        if not isinstance(collateral_symbol, str) or not isinstance(loan_symbol, str):
            raise ValueError(
                f"Morpho market {market_id} on {self._chain} has no collateral/loan "
                f"token symbols; cannot value the position."
            )

        same_asset = collateral_symbol.lower() == loan_symbol.lower()

        # Key by symbol; the seam's ``_resolve_oracle_price`` is case-insensitive.
        if same_asset:
            # Collateral and loan are the same token: one symbol, one price. The HF
            # is price-independent here (the price cancels), so build a single-key
            # dict -- a two-key ``{loan, collateral}`` literal would collapse to one
            # entry and silently drop a lone ``debt_price_usd`` override.
            price = (
                collateral_price_usd
                if collateral_price_usd is not None
                else debt_price_usd
                if debt_price_usd is not None
                else Decimal("1")
            )
            return {collateral_symbol: price}

        # Cross-asset: distinct tokens require distinct, explicit prices -- never
        # default to 1:1 across differing assets. Checked here (after the same-asset
        # branch) so both values narrow to non-None for the return.
        if collateral_price_usd is None or debt_price_usd is None:
            raise ValueError(
                f"Price overrides required for cross-asset Morpho market {market_id}. "
                f"Collateral and debt tokens differ -- cannot default to 1:1."
            )
        return {collateral_symbol: collateral_price_usd, loan_symbol: debt_price_usd}

    # Base tokens that are USD-pegged stablecoins. These are the *only* symbols
    # for which it is safe to assume price == $1 when no external price oracle
    # is provided. Anything else (WETH, AERO, WBTC, etc.) MUST have an
    # explicit ``price_oracle`` or the HF computation fails closed.
    _STABLE_BASE_SYMBOLS: frozenset[str] = frozenset(
        {"USDC", "USDT", "USDS", "USDC.E", "DAI", "FRAX", "LUSD", "USDBC", "SUSDS"}
    )

    def _resolve_base_decimals(self, symbol: str, address: str) -> int:
        """Resolve base-token decimals via the unified TokenResolver.

        Raises on failure. NEVER guesses -- a wrong decimals value silently
        mis-scales debt by orders of magnitude (e.g. WETH=18 vs USDC=6 is a
        1e12 miscalculation). Per CLAUDE.md: "NEVER default to 18 decimals
        - always raise TokenNotFoundError if decimals unknown."
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
                    f"Could not resolve decimals for Compound V3 base token "
                    f"{symbol!r} ({address}) on {self._chain}: {e}. "
                    f"Refusing to guess -- a wrong decimals value silently mis-scales debt."
                ),
            ) from e

    def _resolve_base_price(self, symbol: str) -> Decimal:
        """Resolve USD price for a Compound V3 base token.

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
                    logger.warning(f"Compound V3 PriceOracle({symbol}) failed: {e}")
            # Case 2: plain callable.
            elif callable(oracle):
                try:
                    return Decimal(str(oracle(symbol)))
                except Exception as e:
                    logger.warning(f"Compound V3 price_oracle({symbol}) failed: {e}")

        # Case 3: stablecoin 1:1 fallback.
        if symbol.upper() in self._STABLE_BASE_SYMBOLS:
            return Decimal("1")

        # Case 4: fail closed -- refuse to silently mis-price non-stable bases.
        raise ValueError(
            f"Compound V3 base token {symbol!r} is not a recognized USD stablecoin "
            f"and no working price_oracle was provided. Pass an async PriceOracle "
            f"(with .get_aggregated_price) or a callable(symbol)->Decimal to "
            f"PositionHealthProvider / MarketSnapshot.position_health() to avoid "
            f"inflating reported health factor."
        )

    def _get_compound_health(
        self,
        market_id: str,
        user_address: str,
    ) -> PositionHealth:
        """Compute the multi-collateral Compound V3 (Comet) health factor.

        VIB-4851 PR-2: routes the read through the connector-owned, gateway-routed
        :func:`~almanak.framework.accounting.lending_reads.read_lending_market_health`
        -> :func:`~almanak.connectors.compound_v3.lending_read.read_compound_v3_market_health`.
        That reader preserves the product-owner-chosen *summed* health factor
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
            market_id: Comet market key (e.g. "usdc", "weth").
            user_address: Wallet to inspect.
        """
        from almanak.framework.accounting.lending_reads import read_lending_market_health

        if self._gateway_client is None:
            raise ValueError(
                f"GatewayClient is required to read compound_v3 health on {self._chain}; none was provided."
            )
        if not self._gateway_client.is_connected:
            raise ValueError(f"GatewayClient is not connected; cannot fetch Compound V3 health on {self._chain}.")

        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        # Disambiguate the two fail-closed cases the legacy path raised distinctly
        # (preserved for the byte-equivalence contract): unknown chain vs unknown
        # market. Both lookups are owned by the registry's connector-backed market
        # table now — no private Compound address copy is imported here. The
        # market-scoped ``position_manager_address`` is the chain-level existence
        # signal (truthy when the chain has ANY published Compound market).
        if LendingReadRegistry.market_health_inputs("compound_v3", self._chain, market_id) is None:
            if not LendingReadRegistry.position_manager_address("compound_v3", self._chain):
                raise ValueError(f"Compound V3 not configured for chain: {self._chain}")
            raise ValueError(f"Compound V3 market '{market_id}' not found on {self._chain}.")

        state = read_lending_market_health(
            protocol="compound_v3",
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
                f"Failed to read Compound V3 health for market '{market_id}' on {self._chain} (read returned no data)."
            )
        return self._to_position_health(state, protocol="compound_v3", market_id=market_id)


# =============================================================================
# Unified Health Factor API
# =============================================================================


def _normalize_protocol(protocol: str) -> str:
    """Normalize common protocol name aliases."""
    if protocol is None:
        return ""
    p = protocol.lower().strip()
    aliases = {
        "aave": "aave_v3",
        "aavev3": "aave_v3",
        "aave-v3": "aave_v3",
        "morpho": "morpho_blue",
        "morphoblue": "morpho_blue",
        "morpho-blue": "morpho_blue",
        "compound": "compound_v3",
        "compoundv3": "compound_v3",
        "compound-v3": "compound_v3",
        "comet": "compound_v3",
    }
    return aliases.get(p, p)


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


# Registry of provider factories keyed by normalized protocol name.
# Strategies can register custom providers without forking the SDK.
_HF_FACTORIES: dict[str, Any] = {}


def register_health_factor_provider(protocol: str, factory: Any) -> None:
    """Register a ``HealthFactorProvider`` factory for a protocol.

    The factory is called as ``factory(chain=..., rpc_url=..., gateway_client=...,
    price_oracle=...)`` and must return an object implementing
    :class:`HealthFactorProvider`. Factories are looked up by the normalized
    protocol name (see :func:`_normalize_protocol`).

    Args:
        protocol: Protocol name (e.g. "aave_v3", "morpho_blue", "compound_v3").
        factory: Callable returning a :class:`HealthFactorProvider`.
    """
    _HF_FACTORIES[_normalize_protocol(protocol)] = factory


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
        price_oracle: Optional callable ``symbol -> Decimal`` used by the
            Compound V3 path for base-token pricing.

    Returns:
        ``Decimal`` health factor (``Infinity`` if no debt).

    Raises:
        ValueError: If the protocol is unsupported or no registered factory
            can handle it.
    """
    protocol_n = _normalize_protocol(protocol)
    factory = _HF_FACTORIES.get(protocol_n)
    if factory is not None:
        provider = factory(
            chain=chain,
            rpc_url=rpc_url,
            gateway_client=gateway_client,
            price_oracle=price_oracle,
        )
        return provider.get_health_factor(wallet, market)

    # Default: use the built-in PositionHealthProvider.
    inner = PositionHealthProvider(
        rpc_url=rpc_url,
        chain=chain,
        price_oracle=price_oracle,
        gateway_client=gateway_client,
    )
    adapter = _PositionHealthProviderAdapter(inner, protocol_n)
    return adapter.get_health_factor(wallet, market)


__all__ = [
    "DeleverageTrigger",
    "HealthFactorProvider",
    "PTPositionHealth",
    "PositionHealth",
    "PositionHealthProvider",
    "get_health_factor",
    "register_health_factor_provider",
]
