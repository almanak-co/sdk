"""Framework-wide gas / timeout defaults and read-only derived views.

The framework-default knobs (``DEFAULT_GAS_BUFFER``, ``DEFAULT_TX_TIMEOUT_SECONDS``,
``ANVIL_GAS_PRICE_CAP_GWEI``, …) are literal here because they are *not* per-chain.

Per-chain values are owned by :class:`ChainDescriptor` under
``almanak/core/chains/`` (VIB-4801). Production code reads them via
``ChainRegistry.try_resolve(chain).gas.<field>``. The ``MappingProxyType``
views exposed below (``CHAIN_GAS_BUFFERS`` etc.) are kept solely as a
back-compat surface for regression tests that snapshot the historical
dicts — they have **zero production callers** and must not be re-introduced.
"""

from types import MappingProxyType

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily

# Walk every EVM descriptor once. Solana is intentionally excluded — the
# legacy dicts only covered EVM chains.
_EVM_DESCRIPTORS = [d for d in ChainRegistry.all() if d.family is ChainFamily.EVM]


# =============================================================================
# Gas Limit Buffers (applied to raw gas estimates)
# =============================================================================

# Default gas buffer multiplier for chains not explicitly listed
DEFAULT_GAS_BUFFER: float = 1.2

# Chain-specific gas buffer multipliers for gas limit estimation.
# Applied to raw gas estimates (from simulation or eth_estimateGas) to
# account for L1 data costs on L2s and estimation variance.
CHAIN_GAS_BUFFERS: MappingProxyType[str, float] = MappingProxyType(
    {d.name: d.gas.buffer for d in _EVM_DESCRIPTORS if d.gas.buffer is not None}
)


# =============================================================================
# Simulation Gas Buffers (applied after simulation to account for variance)
# =============================================================================

# Default simulation buffer for chains not explicitly listed
DEFAULT_SIMULATION_BUFFER: float = 0.1

# Chain-specific post-simulation gas buffers (decimal fraction; 0.1 == 10%).
CHAIN_SIMULATION_BUFFERS: MappingProxyType[str, float] = MappingProxyType(
    {d.name: d.gas.simulation_buffer for d in _EVM_DESCRIPTORS if d.gas.simulation_buffer is not None}
)


# =============================================================================
# Recommended Gas Price Caps (gwei)
# =============================================================================

# Default gas price cap for chains not explicitly listed (default 500 GWEI; 0 = no limit)
DEFAULT_GAS_PRICE_CAP_GWEI: int = 500

# Gas price cap used in Anvil mode. Gas costs no real money on Anvil forks,
# so the cap is set very high to avoid blocking development and test workflows.
ANVIL_GAS_PRICE_CAP_GWEI: int = 9999

# VIB-4879: absolute sanity ceiling on any gwei value the system will honour.
# Used by:
#   * the chain-scoped ALMANAK_MAX_GAS_PRICE_GWEI_<CHAIN> env path to clamp
#     operator-supplied values that exceed it (with a WARNING).
#   * the USD-cost cap path to clamp the implicit gwei derived from
#     ALMANAK_MAX_GAS_COST_USD / native_token_price_usd, so a near-zero native
#     price cannot produce an absurd allowed gwei. NOT configurable — this is
#     a product invariant.
SANE_GWEI_CEILING: int = 10_000

# VIB-4879: observed typical-gas snapshot per chain (gwei). Captured against
# each chain's public RPC on 2026-05-27/28 during PR #2476 investigation.
# Two consumers:
#   * tests/unit/core/test_chain_gas_cap_sanity.py — CI gate that pins
#     `descriptor.price_cap_gwei >= 2 × typical` so a future descriptor edit
#     that drops a chain below 2× headroom fails loudly at merge time.
#   * `_warn_if_effective_cap_below_typical_gas` (used at runner boot via
#     `_apply_runtime_gas_risk_overrides`) — operator-visible WARNING when
#     a chain's effective cap (descriptor default or chain-scoped override)
#     is at or below typical live gas, so operators using a stale SDK or a
#     custom override don't silently lose every intent.
# Refresh policy: update both rows AND the snapshot date when re-measuring.
# Chains not in this map have no observable gas at snapshot time (blast,
# monad RPC unreachable in the sweep); the warning helper skips them.
OBSERVED_TYPICAL_GAS_GWEI: MappingProxyType[str, float] = MappingProxyType(
    {
        "ethereum": 0.16,
        "arbitrum": 0.02,
        "base": 0.01,
        "optimism": 0.001,
        "linea": 0.06,
        "polygon": 283.95,
        "bsc": 0.05,
        "avalanche": 0.01,
        "mantle": 50.00,
        "sonic": 55.00,
        "berachain": 0.001,
        "plasma": 0.01,
        "xlayer": 0.02,
        "zerog": 4.00,
    }
)


# Dedupe set for the boot-time effective-cap warning. Keyed by chain name —
# at most one WARNING per chain per process. Cleared in tests via
# ``effective_cap_warned_chains_reset()``.
_EFFECTIVE_CAP_WARNED: set[str] = set()


def effective_cap_warned_chains_reset() -> None:
    """Test-only reset of the warn-once dedupe set."""
    _EFFECTIVE_CAP_WARNED.clear()


def warn_if_effective_cap_below_typical_gas(
    *,
    chain: str,
    effective_cap_gwei: int,
    logger,
    headroom_factor: float = 1.5,
) -> None:
    """Boot-time WARNING when a chain's effective gwei cap is too tight.

    VIB-4879 — operator-visible belt-and-suspenders for the case where
    a chain descriptor has drifted below live gas (or an operator has
    set a chain-scoped override that's too low). The CI sanity test
    (``tests/unit/core/test_chain_gas_cap_sanity.py``) catches descriptor
    drift before merge; this runtime helper catches it for operators
    running a stale SDK or a misconfigured custom cap.

    Args:
        chain: Chain name (lowercase).
        effective_cap_gwei: Post-override gwei cap that will actually
            be enforced for this chain.
        logger: A standard library logger.
        headroom_factor: Cap must be at least ``typical_gas *
            headroom_factor`` to stay silent. Defaults to 1.5x — tight
            enough that the warning fires only on real problems, not
            on every chain that hovers near its cap during normal
            traffic.

    Zero I/O. Reads only the in-process ``OBSERVED_TYPICAL_GAS_GWEI``
    snapshot. Chains without a snapshot entry are silently skipped.
    """
    chain_lower = chain.lower()
    typical = OBSERVED_TYPICAL_GAS_GWEI.get(chain_lower)
    if typical is None:
        return  # no snapshot — quarantined chain, no opinion
    required = typical * headroom_factor
    if effective_cap_gwei >= required:
        return  # healthy headroom

    if chain_lower in _EFFECTIVE_CAP_WARNED:
        return
    _EFFECTIVE_CAP_WARNED.add(chain_lower)

    logger.warning(
        "Chain %s effective max_gas_price_gwei=%d gwei is below "
        "%.1fx typical observed gas (~%.2f gwei). Live traffic may "
        "be blocked. Recommended remediation (one of):"
        " (1) unset any ALMANAK_MAX_GAS_PRICE_GWEI_%s override that is "
        "lower than %.0f gwei;"
        " (2) switch to ALMANAK_MAX_GAS_COST_USD=<dollars> which is "
        "chain-agnostic and uses the live native price;"
        " (3) if the chain descriptor itself is too tight, "
        "update almanak/core/chains/%s.py and re-snapshot "
        "OBSERVED_TYPICAL_GAS_GWEI.",
        chain_lower,
        effective_cap_gwei,
        headroom_factor,
        typical,
        chain_lower.upper(),
        required,
        chain_lower,
    )


# Recommended maximum gas prices per chain (gwei).
# Operators can override via MAX_GAS_PRICE_GWEI env var or config.
CHAIN_GAS_PRICE_CAPS_GWEI: MappingProxyType[str, int] = MappingProxyType(
    {d.name: d.gas.price_cap_gwei for d in _EVM_DESCRIPTORS if d.gas.price_cap_gwei is not None}
)


# =============================================================================
# Recommended Native Gas Cost Caps (in native token units)
# =============================================================================

# Recommended maximum gas cost per transaction in native token units.
# Operators can override via MAX_GAS_COST_NATIVE.
CHAIN_GAS_COST_CAPS_NATIVE: MappingProxyType[str, float] = MappingProxyType(
    {d.name: d.gas.cost_cap_native for d in _EVM_DESCRIPTORS if d.gas.cost_cap_native is not None}
)


# =============================================================================
# Transaction Confirmation Timeouts (seconds)
# =============================================================================

# Default transaction confirmation timeout for chains not explicitly listed
DEFAULT_TX_TIMEOUT_SECONDS: int = 120

# Chain-specific transaction confirmation timeouts.
# Ethereum L1 has 12s block times and multi-tx strategies may need 3+ blocks,
# plus gas price volatility can delay inclusion. L2s are much faster (~2s blocks).
CHAIN_TX_TIMEOUTS: MappingProxyType[str, int] = MappingProxyType(
    {d.name: d.timeouts.tx_confirmation for d in _EVM_DESCRIPTORS if d.timeouts.tx_confirmation is not None}
)


# =============================================================================
# gRPC Execute Call Timeouts (seconds)
# =============================================================================

# Default gRPC timeout for the Execute call (covers gas estimation + submission + TX confirmation).
# Must be larger than CHAIN_TX_TIMEOUTS to account for overhead before TX is submitted.
# Overhead components: gas estimation (LocalSimulator snapshot+execute), signing, submission.
DEFAULT_GRPC_EXECUTE_TIMEOUT_SECONDS: int = 300

# Chain-specific gRPC Execute timeouts.
# Rule of thumb: CHAIN_TX_TIMEOUTS[chain] + 180s overhead for gas estimation + processing.
CHAIN_GRPC_EXECUTE_TIMEOUTS: MappingProxyType[str, int] = MappingProxyType(
    {d.name: d.timeouts.grpc_execute for d in _EVM_DESCRIPTORS if d.timeouts.grpc_execute is not None}
)
