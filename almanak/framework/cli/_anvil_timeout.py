"""Per-chain Anvil startup-timeout calculation (VIB-3877).

Both ``cli/run_helpers.py`` (``strat run``) and ``cli/teardown.py``
(``strat teardown execute``) need to size the ``ManagedGateway.start(timeout=...)``
budget against the set of Anvil forks the gateway will boot. Cold-cache
archive-RPC chains (Ethereum, Polygon, Avalanche) take 60-90s per fork; L2s
take ~30s. Multi-chain configs (e.g. ``[ethereum, polygon]``) need the budget
summed because ``ManagedGateway`` boots forks sequentially.

Pulling this into a single helper keeps the policy in one place, sourced from
``ManagedGateway.ARCHIVE_RPC_REQUIRED_CHAINS`` so the gateway itself owns the
slow-chain set. Two call-site copies will silently drift the next time the
policy changes (e.g. when ``bsc`` joins the slow set).
"""

from __future__ import annotations

GATEWAY_WARMUP_HEADROOM_SECONDS = 30.0
"""Seconds added to the per-fork budget for the gateway-server warmup +
prewarm phase that runs after all Anvil forks are ready."""

L2_FORK_BUDGET_SECONDS = 30.0
"""Per-fork startup budget for an L2 chain (Base, Arbitrum, Optimism, Linea,
Mantle, BSC, ...). Empirically sufficient for cold-cache fork startup against
public RPC endpoints."""

ARCHIVE_RPC_FORK_BUDGET_SECONDS = 90.0
"""Per-fork startup budget for an archive-RPC chain (Ethereum, Polygon,
Avalanche). Cold-cache fork against an archive node can take 60-90s."""


def _canonical_chain(chain: str) -> str:
    """Canonicalize chain aliases (``avax`` -> ``avalanche``, ``eth`` -> ``ethereum``).

    Falls back to ``chain.strip().lower()`` when the alias is unknown — that
    gives the slow-chain check a non-False answer for valid-looking-but-
    unrecognized chain names rather than silently classifying them as fast.
    """
    from almanak.core.constants import resolve_chain_name

    try:
        return resolve_chain_name(chain)
    except ValueError:
        return chain.strip().lower()


def compute_anvil_startup_timeout(anvil_chains: list[str]) -> float:
    """Return the ``ManagedGateway.start(timeout=...)`` budget for ``anvil_chains``.

    For each chain, allocate :data:`ARCHIVE_RPC_FORK_BUDGET_SECONDS` if the chain
    is in :attr:`ManagedGateway.ARCHIVE_RPC_REQUIRED_CHAINS` (sourced live so
    this helper can never drift), else :data:`L2_FORK_BUDGET_SECONDS`. Add a
    flat :data:`GATEWAY_WARMUP_HEADROOM_SECONDS` for the gateway-server start
    that follows.

    When ``anvil_chains`` is empty (``--network mainnet`` or unsupported chain),
    fall back to a 10s no-fork budget — the gateway just starts the gRPC
    server, no fork work to wait on.

    Mirrors the comment block in ``run_helpers.py`` that documents the
    cold-cache failure mode this guards against (``absl::InitializeLog()
    called multiple times`` race).

    Args:
        anvil_chains: Chains the gateway will fork. May be empty.

    Returns:
        Timeout budget in seconds, suitable for ``managed_gateway.start(timeout=...)``.
    """
    if not anvil_chains:
        return 10.0

    from almanak.gateway.managed import ManagedGateway

    slow_chains = ManagedGateway.ARCHIVE_RPC_REQUIRED_CHAINS
    fork_budget = sum(
        ARCHIVE_RPC_FORK_BUDGET_SECONDS if _canonical_chain(c) in slow_chains else L2_FORK_BUDGET_SECONDS
        for c in anvil_chains
    )
    return fork_budget + GATEWAY_WARMUP_HEADROOM_SECONDS


__all__ = [
    "ARCHIVE_RPC_FORK_BUDGET_SECONDS",
    "GATEWAY_WARMUP_HEADROOM_SECONDS",
    "L2_FORK_BUDGET_SECONDS",
    "compute_anvil_startup_timeout",
]
