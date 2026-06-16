"""Intent error and exception classes.

Custom exceptions raised during intent validation, chain resolution,
and protocol parameter checking.
"""

from collections.abc import Sequence
from typing import Any


class InvalidChainError(ValueError):
    """Raised when an intent specifies a chain not configured for the strategy.

    Attributes:
        chain: The invalid chain that was specified
        configured_chains: The list of chains configured for the strategy
    """

    def __init__(self, chain: str, configured_chains: Sequence[str]) -> None:
        self.chain = chain
        self.configured_chains = list(configured_chains)
        chains_str = ", ".join(sorted(self.configured_chains)) if self.configured_chains else "(none)"
        super().__init__(f"Chain '{chain}' is not configured for this strategy. Configured chains: {chains_str}")


class InvalidSequenceError(ValueError):
    """Raised when an intent sequence is invalid.

    Attributes:
        message: Description of the error
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class InvalidAmountError(ValueError):
    """Raised when amount='all' is used incorrectly.

    The 'all' amount is only valid when chaining outputs from a previous step.
    Using amount='all' on the first step of a sequence or on a standalone intent
    is invalid because there is no previous step output to reference.

    Attributes:
        intent_type: Type of intent with invalid amount
        reason: Explanation of why the amount is invalid
    """

    def __init__(self, intent_type: str, reason: str) -> None:
        self.intent_type = intent_type
        self.reason = reason
        super().__init__(f"Invalid amount='all' for {intent_type}: {reason}")


class InvalidProtocolParameterError(ValueError):
    """Raised when a protocol-specific parameter is invalid or not supported.

    Protocol-specific parameters are validated against the protocol's capabilities.
    For example, Aave supports 'variable' interest rate mode, while
    other protocols may not support interest rate mode selection at all.

    Attributes:
        protocol: The protocol that doesn't support the parameter
        parameter: The parameter name that is invalid
        value: The value that was provided
        reason: Explanation of why the parameter is invalid
    """

    def __init__(self, protocol: str, parameter: str, value: Any, reason: str) -> None:
        self.protocol = protocol
        self.parameter = parameter
        self.value = value
        self.reason = reason
        super().__init__(f"Invalid protocol parameter for '{protocol}': {parameter}={value!r}. {reason}")


class ProtocolRequiredError(ValueError):
    """Raised when protocol parameter is required but not provided.

    When a chain has multiple protocols configured that support the same operation,
    the protocol parameter must be explicitly specified to avoid ambiguity.

    Attributes:
        operation: The operation being performed (e.g., "borrow", "supply")
        available_protocols: List of protocols that support this operation on the chain
    """

    def __init__(self, operation: str, available_protocols: list[str]) -> None:
        self.operation = operation
        self.available_protocols = available_protocols
        protocols_str = ", ".join(sorted(available_protocols))
        super().__init__(
            f"Protocol must be specified for '{operation}' operation. Available protocols: {protocols_str}"
        )


class LpOpenZeroLiquidityError(ValueError):
    """Raised when an LP_OPEN intent would mint zero liquidity.

    Uniswap V3's ``UniswapV3Pool.mint()`` requires the computed liquidity to be
    strictly greater than zero (``M0`` revert string when violated). For
    near-1:1 pegged pairs (stETH/WETH, frxETH/WETH, etc.) on tight tick
    ranges, the ``getLiquidityForAmounts`` math can floor-divide to zero
    even when both ``amount0_desired`` and ``amount1_desired`` are positive.

    This pre-flight is the framework-level fix mirroring the
    VIB-3744 / VIB-3749 "fail before on-chain" pattern: surface a typed
    error at compile time so strategies can react cleanly (widen the
    range, hold for the next iteration, choose a different fee tier)
    instead of burning gas on the on-chain ``M0`` revert.

    Attributes:
        amount0_desired: Token0 amount in wei that was supplied.
        amount1_desired: Token1 amount in wei that was supplied.
        tick_lower: Lower tick of the requested LP range (spacing-aligned).
        tick_upper: Upper tick of the requested LP range (spacing-aligned).
        reason: Human-facing diagnostic string (range-too-narrow vs.
            amounts-too-small).

    Strategies can match on the stable error-message prefix
    (``"LP_OPEN would mint zero liquidity"``) returned in
    ``CompilationResult.error`` to emit a clean ``Intent.hold(...)``.
    """

    ERROR_PREFIX = "LP_OPEN would mint zero liquidity"

    def __init__(
        self,
        *,
        amount0_desired: int,
        amount1_desired: int,
        tick_lower: int,
        tick_upper: int,
        reason: str,
    ) -> None:
        self.amount0_desired = amount0_desired
        self.amount1_desired = amount1_desired
        self.tick_lower = tick_lower
        self.tick_upper = tick_upper
        self.reason = reason
        super().__init__(
            f"{self.ERROR_PREFIX} at the requested tick range "
            f"[{tick_lower}, {tick_upper}] with amounts "
            f"({amount0_desired}, {amount1_desired}). {reason}"
        )


class LendingBorrowNotEnabledError(ValueError):
    """Raised when a BORROW intent targets a reserve with ``borrowingEnabled=false``.

    Aave V3 (and V2-fork) reserves can be flagged supply-only by governance —
    every BORROW against such an asset reverts on-chain with short-string code
    ``11`` (``BORROWING_NOT_ENABLED``). The compile-time pre-flight in
    :func:`almanak.connectors._strategy_base.base.lending.aave_helpers._check_lending_reserve_borrowable`
    fires this typed error so strategies can match on the stable error-message
    prefix and emit ``Intent.hold(...)`` instead of burning gas on the
    on-chain revert.

    Concrete trigger: ``aave_v3_aerodrome_leveraged_lp_base`` (USDC borrow on
    Base) per the QA April-31 harness, BUG-35 / VIB-3825.

    Attributes:
        chain: The chain the BORROW targets.
        protocol: The lending protocol (e.g. ``"aave_v3"``).
        asset_symbol: The borrow asset symbol that is not borrowable.
        asset_address: The borrow asset's contract address.
        reason: Human-facing diagnostic message including the
            ``borrowingEnabled=False`` flag.

    Strategies can match on the stable error-message prefix
    (``"Lending borrow not enabled"``) returned in
    ``CompilationResult.error`` to emit a clean ``Intent.hold(...)``.
    """

    ERROR_PREFIX = "Lending borrow not enabled"

    def __init__(
        self,
        *,
        chain: str,
        protocol: str,
        asset_symbol: str,
        asset_address: str,
        reason: str,
    ) -> None:
        self.chain = chain
        self.protocol = protocol
        self.asset_symbol = asset_symbol
        self.asset_address = asset_address
        self.reason = reason
        super().__init__(f"{self.ERROR_PREFIX} for {asset_symbol} on {protocol} {chain}: {reason}")


class LendingBorrowExceedsCapacityError(ValueError):
    """Raised when a BORROW intent requests more than the wallet's available borrow capacity.

    The wallet's collateral and the protocol's per-asset oracle price together
    define an upper bound on how much of any borrow asset the wallet can draw
    without immediately tripping the protocol's collateralization check. When
    the requested amount exceeds this bound the on-chain borrow always
    reverts (Aave V3 short-string code ``35`` ``COLLATERAL_CANNOT_COVER_NEW_BORROW``;
    Compound V2 / BENQI Comptroller error code ``4`` ``INSUFFICIENT_LIQUIDITY``)
    after burning gas + retry iterations.
    The compile-time pre-flight in
    :func:`almanak.connectors._strategy_base.base.lending.aave_helpers._check_lending_borrow_capacity`
    fires this typed error so strategies can match on the stable error-message
    prefix and emit ``Intent.hold(...)`` instead of looping on the on-chain
    revert.
    Defense in depth — mainnet protocol enforcement is presumed correct
    (Compound V2 ``borrowAllowed`` / Aave V3 ``executeBorrow``), but this
    pre-flight makes the revert observable at compile time so the runner's
    permanent-error classifier can prevent retry storms.
    Attributes:
        chain: The chain the BORROW targets.
        protocol: The lending protocol (e.g. ``"benqi"``, ``"aave_v3"``).
        asset_symbol: The borrow asset symbol that exceeds capacity.
        asset_address: The borrow asset's contract address.
        requested_amount: The borrow amount the strategy asked for.
        available_amount: The capacity the pre-flight computed (in the same
            asset's underlying decimals).
        reason: Human-facing diagnostic message including both numbers.
    Strategies can match on the stable error-message prefix
    (``"Lending borrow exceeds capacity"``) returned in
    ``CompilationResult.error`` to emit a clean ``Intent.hold(...)``.
    """

    ERROR_PREFIX = "Lending borrow exceeds capacity"

    def __init__(
        self,
        *,
        chain: str,
        protocol: str,
        asset_symbol: str,
        asset_address: str,
        requested_amount: Any,
        available_amount: Any,
        reason: str,
    ) -> None:
        self.chain = chain
        self.protocol = protocol
        self.asset_symbol = asset_symbol
        self.asset_address = asset_address
        self.requested_amount = requested_amount
        self.available_amount = available_amount
        self.reason = reason
        super().__init__(f"{self.ERROR_PREFIX} for {asset_symbol} on {protocol} {chain}: {reason}")


class BundledCollateralBorrowError(ValueError):
    """Raised when a lending BORROW intent bundles a collateral supply (``collateral_amount > 0``).

    A lending ``BorrowIntent`` that carries ``collateral_amount > 0`` (or the
    chained ``collateral_amount="all"`` form, which resolves to a positive
    supply at execution time) supplies **and** borrows on-chain in a single
    action. But the accounting layer writes exactly **one** ``accounting_events``
    row per intent (one ``transaction_ledger`` row → one event,
    ``AccountingProcessor.drain_one``). The supply leg therefore collapses into
    the single BORROW event: no standalone SUPPLY accounting event is written
    and no ``supply:<position_key>`` FIFO cost-basis lot is recorded. A later
    WITHDRAW that closes the position has no supply lot to match interest
    against, so principal/interest attribution is wrong and deployed collateral
    is under-reported.

    This is the 1:1 ledger→event invariant (load-bearing across
    accounting/reconciliation) being violated at the *event* level without
    violating the *intent* count. Until a compiler-level decomposition exists
    that emits the supply and borrow as two ledger rows, the production-safe
    behaviour is to fail closed (loud reject) and steer callers to the
    accounting-correct two-intent form.

    See ``docs/internal/bundled-collateral-borrow-migration.md`` for the guard,
    carve-outs, and migration tracker (and ``docs/internal/FollowUp-13June15.md``
    §D1 / VIB-3586 ``9d982cadf`` for the original root cause).

    Attributes:
        protocol: The lending protocol the borrow targets (e.g. ``"aave_v3"``).
        collateral_token: The collateral token the bundled borrow tried to supply.
        collateral_amount: The bundled collateral amount that triggered the guard
            (a positive ``Decimal`` or the literal ``"all"``).
        borrow_token: The token the intent tried to borrow.

    Strategies/callers can match on the stable error-message prefix
    (``"Bundled collateralized borrow is not supported"``) returned in
    ``CompilationResult.error``.
    """

    ERROR_PREFIX = "Bundled collateralized borrow is not supported"

    def __init__(
        self,
        *,
        protocol: str,
        collateral_token: str,
        collateral_amount: Any,
        borrow_token: str,
    ) -> None:
        self.protocol = protocol
        self.collateral_token = collateral_token
        self.collateral_amount = collateral_amount
        self.borrow_token = borrow_token
        super().__init__(
            f"{self.ERROR_PREFIX} for accounting-correct lending "
            f"(protocol={protocol!r}, collateral_amount={collateral_amount!r}). "
            f"A single bundled borrow supplies and borrows on-chain in one action, but accounting "
            f"writes one event per intent — the SUPPLY accounting event and the supply cost-basis lot "
            f"are dropped, corrupting principal/interest attribution. "
            f"Emit Intent.supply(protocol={protocol!r}, token={collateral_token!r}, "
            f"amount=<collateral_amount>, use_as_collateral=True) first, then "
            f"Intent.borrow(protocol={protocol!r}, collateral_token={collateral_token!r}, "
            f'collateral_amount=Decimal("0"), borrow_token={borrow_token!r}, borrow_amount=<amount>). '
            f"(See docs/internal/bundled-collateral-borrow-migration.md.)"
        )


class InvalidCollateralForMarketError(ValueError):
    """Raised when a perp intent specifies a collateral that is invalid for the market.

    Perpetuals protocols like GMX V2 bind each market to a fixed pair of
    collateral tokens (the market's ``longToken`` and ``shortToken``). Orders
    opened with any other collateral are silently cancelled by keepers and the
    keeper execution fee is burned. We validate this pair at compile time so
    that strategies fail fast with a clear error instead of burning fees on a
    cancelled order.

    Attributes:
        market: The market identifier (e.g. ``"SOL/USD"``).
        collateral: The invalid collateral token that was supplied.
        allowed_collaterals: Collateral token symbols that the market actually
            accepts (usually the ``longToken`` and ``shortToken``).
        chain: Optional chain the market lives on (``"arbitrum"``, ``"avalanche"``).
        protocol: Optional protocol identifier (defaults to ``"gmx_v2"``).
    """

    def __init__(
        self,
        market: str,
        collateral: str,
        allowed_collaterals: list[str],
        chain: str | None = None,
        protocol: str | None = None,
    ) -> None:
        self.market = market
        self.collateral = collateral
        self.allowed_collaterals = list(allowed_collaterals)
        self.chain = chain
        self.protocol = protocol
        allowed_str = ", ".join(self.allowed_collaterals) if self.allowed_collaterals else "(none)"
        proto_str = f"{protocol} " if protocol else ""
        chain_str = f" on {chain}" if chain else ""
        super().__init__(
            f"Invalid collateral '{collateral}' for {proto_str}market '{market}'{chain_str}. "
            f"Allowed collaterals: {allowed_str}. "
            f"Orders with invalid collateral are cancelled by keepers and the execution fee is burned."
        )
