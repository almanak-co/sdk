"""Byte-equivalence pin for VIB-4858 W6 GasEstimateCapability refactor.

Locks the contract that ``get_gas_estimate(chain, operation)`` returns
the **exact** integer for every ``(operation)`` triple the pre-W6
central ``DEFAULT_GAS_ESTIMATES`` dict held. After W6 the per-protocol
half of that dict moved onto each owning connector's
``gas_estimate_provider.py``; this test asserts no number drifted in
the move.

A non-test verification script
(``/tmp/verify_w6.py`` in the original W6 implementation) ran a
finer-grained sweep across every chain in ``ChainRegistry`` and the
``__no_override__`` virtual chain to catch per-chain overrides
specifically. That script is intentionally not committed (it relies on
``git show origin/main:…``). This test pins the protocol-owned action
gas estimates as the **permanent** in-tree byte-equivalence guard.

If you intentionally change a connector's gas number, you MUST update
the corresponding expected value here AND document the rationale on the
connector's ``gas_estimate_provider.py`` module — production strategies
have been tuned against these defaults.
"""

from __future__ import annotations

import pytest

from almanak.framework.intents.compiler_constants import (
    DEFAULT_GAS_ESTIMATES,
    get_gas_estimate,
)


# Pre-W6 ``DEFAULT_GAS_ESTIMATES`` snapshot (21 entries) — every protocol-
# owned action and every baseline action. The owner column documents the
# post-W6 owner; ``baseline`` means the value stays in
# ``compiler_constants.DEFAULT_GAS_ESTIMATES``.
PRE_W6_DEFAULT_GAS_ESTIMATES: dict[str, tuple[int, str]] = {
    # Chain-level common primitives — owner = baseline.
    "approve": (80000, "baseline"),
    "swap_simple": (200000, "baseline"),
    "swap_multi_hop": (350000, "baseline"),
    "wrap_eth": (30000, "baseline"),
    "unwrap_eth": (30000, "baseline"),
    # Uniswap V3 — concentrated-liquidity LP actions (canonical CL DEX
    # baseline; UniV3 forks inherit via the shared compiler path).
    "lp_mint": (500000, "uniswap_v3"),
    "lp_increase_liquidity": (200000, "uniswap_v3"),
    "lp_decrease_liquidity": (250000, "uniswap_v3"),
    "lp_collect": (200000, "uniswap_v3"),
    "lp_burn": (100000, "uniswap_v3"),
    # Aave V3 — lending + flash loans.
    "lending_supply": (300000, "aave_v3"),
    "lending_borrow": (450000, "aave_v3"),
    "lending_repay": (250000, "aave_v3"),
    "lending_withdraw": (250000, "aave_v3"),
    "flash_loan": (500000, "aave_v3"),
    "flash_loan_simple": (300000, "aave_v3"),
    # Balancer V2 — namespaced flash loans (zero-fee, batch-native).
    "balancer_flash_loan": (400000, "balancer_v2"),
    "balancer_flash_loan_simple": (250000, "balancer_v2"),
    # Across — cross-chain bridge deposit (quote-dependent, up to 675K
    # on some destinations).
    "bridge_deposit": (800000, "across"),
    # MetaMorpho vaults — ERC-4626 deposit / multi-market redeem.
    "vault_deposit": (200000, "metamorpho"),
    "vault_redeem": (250000, "metamorpho"),
}


# ---------------------------------------------------------------------------
# Byte-equivalence assertions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "operation,expected,owner",
    [(op, exp, owner) for op, (exp, owner) in PRE_W6_DEFAULT_GAS_ESTIMATES.items()],
    ids=list(PRE_W6_DEFAULT_GAS_ESTIMATES),
)
def test_get_gas_estimate_byte_equivalent_no_chain_override(
    operation: str, expected: int, owner: str
) -> None:
    """``get_gas_estimate`` returns the pre-W6 integer for every action.

    Uses ``arbitrum`` as the test chain because it has **no** per-chain
    operation_overrides (verified by the W5 chain-descriptor tests), so
    the lookup falls straight through the per-chain override branch and
    asks the W6 registry / baseline table.
    """
    assert get_gas_estimate("arbitrum", operation) == expected, (
        f"action {operation!r} (owner: {owner}) returned a different gas "
        f"estimate post-W6 — drift detected. Update the owner's "
        f"gas_estimate_provider.py or this pin, NEVER both silently."
    )


def test_unknown_action_falls_back_to_120000() -> None:
    """The pre-W6 ``dict.get(operation, 120000)`` semantics are preserved."""
    assert get_gas_estimate("arbitrum", "__nonsense_action__") == 120000


def test_default_gas_estimates_back_compat_full_dict_view() -> None:
    """Public ``DEFAULT_GAS_ESTIMATES`` preserves the pre-W6 dict-indexing API.

    External SDK consumers may import ``DEFAULT_GAS_ESTIMATES`` and index
    protocol actions (``DEFAULT_GAS_ESTIMATES["lp_mint"]``) directly. W6
    moved those entries onto each connector's gas_estimate_provider but
    rebuilds the merged dict (baseline ∪ every registered connector's
    actions) for back-compat. This test pins both the keyset and the
    integer for every pre-W6 entry.
    """
    expected = {op: value for op, (value, _owner) in PRE_W6_DEFAULT_GAS_ESTIMATES.items()}
    assert dict(DEFAULT_GAS_ESTIMATES) == expected, (
        "DEFAULT_GAS_ESTIMATES drift detected. Either a connector dropped "
        "an action it used to publish, or the baseline dict changed. The "
        "post-W6 back-compat contract is that the merged dict reproduces "
        "the pre-W6 21-entry shape byte-equivalent."
    )


def test_registry_publishes_every_per_protocol_action() -> None:
    """Every non-baseline action must have an owner registered.

    Catches the reverse of the previous test: a per-protocol action got
    removed from a connector's gas_estimate_provider but the byte-pin
    here still expects an owner.
    """
    from almanak.connectors._strategy_gas_estimate_registry import (
        STRATEGY_GAS_ESTIMATE_REGISTRY,
    )

    expected_per_protocol = {
        op for op, (_value, owner) in PRE_W6_DEFAULT_GAS_ESTIMATES.items() if owner != "baseline"
    }
    registered = STRATEGY_GAS_ESTIMATE_REGISTRY.actions()
    missing = expected_per_protocol - registered
    assert not missing, (
        f"Per-protocol actions missing from STRATEGY_GAS_ESTIMATE_REGISTRY: "
        f"{sorted(missing)}. Either re-register them via the owning "
        f"connector's gas_estimate_provider.py or update this pin."
    )


def test_registry_publishes_no_baseline_action() -> None:
    """Baseline actions must NOT be claimed by any connector.

    Two-way contract: baseline lives in compiler_constants; per-protocol
    lives in the registry. A connector that claims a baseline key (e.g.
    publishing ``approve`` as a per-connector estimate) would break the
    consumer's fallback order — that's why we make it a hard test.
    """
    from almanak.connectors._strategy_gas_estimate_registry import (
        STRATEGY_GAS_ESTIMATE_REGISTRY,
    )

    baseline_keys = {
        op for op, (_value, owner) in PRE_W6_DEFAULT_GAS_ESTIMATES.items() if owner == "baseline"
    }
    registered = STRATEGY_GAS_ESTIMATE_REGISTRY.actions()
    overlap = baseline_keys & registered
    assert not overlap, (
        f"Baseline keys also published by a connector: {sorted(overlap)}. "
        f"Move them off the gas_estimate_provider or remove them from the "
        f"baseline DEFAULT_GAS_ESTIMATES — they cannot live in both places."
    )


def test_registry_action_owner_matches_pin() -> None:
    """Every protocol-owned action's connector identity matches the pin.

    Catches a connector accidentally claiming the wrong action (e.g.
    Aave's provider publishing ``balancer_flash_loan``).
    """
    from almanak.connectors._strategy_gas_estimate_registry import (
        STRATEGY_GAS_ESTIMATE_REGISTRY,
    )

    for op, (_value, owner) in PRE_W6_DEFAULT_GAS_ESTIMATES.items():
        if owner == "baseline":
            continue
        connector = STRATEGY_GAS_ESTIMATE_REGISTRY.action_owner(op)
        assert connector is not None, f"no owner for {op!r}"
        assert connector.protocol == owner, (
            f"action {op!r} expected to be owned by {owner!r} but is owned "
            f"by {connector.protocol!r} ({type(connector).__qualname__})"
        )
