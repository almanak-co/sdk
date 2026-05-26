"""Orca Whirlpools exceptions."""


class OrcaError(Exception):
    """Base exception for Orca operations."""


class OrcaAPIError(OrcaError):
    """Error communicating with the Orca API."""

    def __init__(
        self,
        message: str,
        status_code: int = 0,
        endpoint: str = "",
    ) -> None:
        self.status_code = status_code
        self.endpoint = endpoint
        super().__init__(message)


class OrcaConfigError(OrcaError):
    """Invalid Orca configuration."""

    def __init__(self, message: str, parameter: str = "") -> None:
        self.parameter = parameter
        super().__init__(message)


class OrcaPoolError(OrcaError):
    """Error with pool state or operations."""


class OrcaTickArrayUninitializedError(OrcaError):
    """Raised when an LP_OPEN would target an uninitialized tick array PDA.

    Orca Whirlpools' ``increase_liquidity`` CPI requires the lower-bound and
    upper-bound tick array accounts to already exist on-chain (program error
    ``0xbbf`` / 3007 — ``InitializedTickArrayNotFound``). On forked validators
    that didn't clone the tick-array PDAs (cf. VIB-3753 cloned the program but
    not its child PDAs), or on real mainnet ranges that fall outside currently
    active tick arrays, the open-position transaction would burn gas on the
    on-chain revert.

    This pre-flight is the framework-level fix mirroring the
    VIB-3744 / VIB-3815 / VIB-3823 "fail before on-chain" pattern: surface a
    typed error at compile time so strategies can react cleanly (widen the
    range, fall back to a market-spanning range, hold for the next iteration)
    instead of burning gas on the on-chain ``0xbbf`` revert.

    Attributes:
        pool_address: The Orca Whirlpool the position would target.
        tick_lower: Lower tick of the requested LP range (spacing-aligned).
        tick_upper: Upper tick of the requested LP range.
        missing_tick_arrays: PDAs (base58) that came back as not-initialized.

    Strategies can match on the stable error-message prefix
    (``"Orca tick array(s) not initialized"``) returned in the compiler's
    error bundle to emit a clean ``Intent.hold(...)`` or widen the range.
    """

    # NOTE: avoid the literal "revert" anywhere in the rendered message — the
    # state machine classifies any error containing "revert" as transient
    # REVERT *before* the COMPILATION_PERMANENT keyword table is consulted.
    # Same hazard already pinned for VIB-3828 (Enso router rejected route).
    ERROR_PREFIX = "Orca tick array(s) not initialized"

    def __init__(
        self,
        *,
        pool_address: str,
        tick_lower: int,
        tick_upper: int,
        missing_tick_arrays: list[str],
    ) -> None:
        self.pool_address = pool_address
        self.tick_lower = tick_lower
        self.tick_upper = tick_upper
        self.missing_tick_arrays = list(missing_tick_arrays)
        super().__init__(
            f"{self.ERROR_PREFIX} on pool {pool_address[:8]}... "
            f"for tick range [{tick_lower}, {tick_upper}]. "
            f"Uninitialized PDAs: {', '.join(p[:8] + '...' for p in missing_tick_arrays)}. "
            f"On-chain Whirlpool returns 0xbbf "
            f"(InitializedTickArrayNotFound). Widen the range, choose a more "
            f"liquid pool, or wait for a tick array in this band to be opened."
        )
