"""VIB-3877 — Per-chain Anvil startup-timeout helper.

Pins the budget policy used by both ``strat run`` (run_helpers.py) and ``strat
teardown execute`` (cli/teardown.py) so the two cannot drift when the slow-
chain set or per-fork budgets change.

Failure modes guarded:

- L2 underestimate → cold-cache fork hangs past the timeout, gateway aborts
  with a daemon gRPC thread still running.
- Archive-chain underestimate → same race; ``absl::InitializeLog() called
  multiple times`` on the next CLI run because the half-shutdown gateway
  re-initializes log state on the second start.
- Multi-chain undercount → ``ManagedGateway`` boots forks sequentially; if the
  budget is sized for one fork, the second one runs out of time.
"""

from __future__ import annotations

import pytest

from almanak.framework.cli._anvil_timeout import (
    ARCHIVE_RPC_FORK_BUDGET_SECONDS,
    GATEWAY_WARMUP_HEADROOM_SECONDS,
    L2_FORK_BUDGET_SECONDS,
    compute_anvil_startup_timeout,
)
from almanak.gateway.managed import ManagedGateway


def test_empty_anvil_chains_returns_no_fork_budget() -> None:
    """No forks → no fork budget; just the 10s gRPC server start budget."""
    assert compute_anvil_startup_timeout([]) == 10.0


def test_single_l2_chain_gets_l2_budget_plus_headroom() -> None:
    """One L2 fork → one L2 budget + warmup headroom."""
    assert compute_anvil_startup_timeout(["base"]) == L2_FORK_BUDGET_SECONDS + GATEWAY_WARMUP_HEADROOM_SECONDS


@pytest.mark.parametrize("archive_chain", sorted(ManagedGateway.ARCHIVE_RPC_REQUIRED_CHAINS))
def test_archive_chain_gets_archive_budget(archive_chain: str) -> None:
    """Every chain in the gateway's slow-set must get the archive budget.

    Parameterized so when ``ManagedGateway.ARCHIVE_RPC_REQUIRED_CHAINS`` adds
    a new entry (the original drift risk that motivated this helper), the
    test catches any path that bypasses the policy.
    """
    expected = ARCHIVE_RPC_FORK_BUDGET_SECONDS + GATEWAY_WARMUP_HEADROOM_SECONDS
    assert compute_anvil_startup_timeout([archive_chain]) == expected


def test_multi_chain_budget_is_summed() -> None:
    """``ManagedGateway`` boots forks sequentially → budgets must sum."""
    expected = (
        ARCHIVE_RPC_FORK_BUDGET_SECONDS  # ethereum
        + L2_FORK_BUDGET_SECONDS  # base
        + GATEWAY_WARMUP_HEADROOM_SECONDS
    )
    assert compute_anvil_startup_timeout(["ethereum", "base"]) == expected


def test_multi_archive_chain_budget_is_summed() -> None:
    """Worst case: 2 archive forks back-to-back. Budget must accommodate both."""
    expected = 2 * ARCHIVE_RPC_FORK_BUDGET_SECONDS + GATEWAY_WARMUP_HEADROOM_SECONDS
    assert compute_anvil_startup_timeout(["ethereum", "polygon"]) == expected


def test_chain_alias_canonicalized_to_archive() -> None:
    """``avax`` → ``avalanche`` (archive-RPC), ``eth`` → ``ethereum`` (archive-RPC)."""
    expected = ARCHIVE_RPC_FORK_BUDGET_SECONDS + GATEWAY_WARMUP_HEADROOM_SECONDS
    assert compute_anvil_startup_timeout(["avax"]) == expected
    assert compute_anvil_startup_timeout(["eth"]) == expected


def test_unknown_chain_falls_through_as_l2() -> None:
    """Unknown chain alias gets L2 budget (safer than misclassifying as archive)."""
    expected = L2_FORK_BUDGET_SECONDS + GATEWAY_WARMUP_HEADROOM_SECONDS
    assert compute_anvil_startup_timeout(["definitely-not-a-chain"]) == expected


def test_chain_case_insensitive() -> None:
    """Canonical chain names are lowercase; mixed-case input still matches."""
    expected = ARCHIVE_RPC_FORK_BUDGET_SECONDS + GATEWAY_WARMUP_HEADROOM_SECONDS
    assert compute_anvil_startup_timeout(["Ethereum"]) == expected
    assert compute_anvil_startup_timeout(["POLYGON"]) == expected


def test_helper_sources_slow_set_from_managed_gateway() -> None:
    """If the gateway adds a new archive-RPC chain, the helper must follow.

    Pin contract: the helper must consult
    ``ManagedGateway.ARCHIVE_RPC_REQUIRED_CHAINS`` rather than redeclaring its
    own copy. Tested by re-classifying the budget for every chain in the set.
    """
    for chain in ManagedGateway.ARCHIVE_RPC_REQUIRED_CHAINS:
        budget = compute_anvil_startup_timeout([chain])
        assert budget == ARCHIVE_RPC_FORK_BUDGET_SECONDS + GATEWAY_WARMUP_HEADROOM_SECONDS, (
            f"Slow-chain {chain!r} must get the archive-RPC budget; got {budget}s"
        )
