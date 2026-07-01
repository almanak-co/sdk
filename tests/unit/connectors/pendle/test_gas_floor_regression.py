"""Regression guard for the Pendle heavy-vault-SY gas floors (VIB-5487).

BUG A was a live LP_CLOSE ``removeLiquiditySingleToken`` reverting with
``SafeERC20: low-level call failed`` on the sUSDai market. The true fault was
``OutOfGas`` deep in the SY -> sUSDai redeem (blocklist-check + vault-withdraw
frames); the 63/64 gas rule surfaced it as a SafeERC20 low-level failure rather
than a top-level OOG. The tx ran with a 600k limit (the then-current 400k floor
× the 1.5 chain buffer) while the remove needs ~608k, so it starved.

The floor had been raised (#3088) but shipped with NO test asserting it actually
covers the measured heavy-vault-SY requirement — which is exactly how the
under-provisioned 400k floor shipped in the first place. This guard closes that
gap: it pins every heavy single-token Pendle op floor at or above its measured
worst-observed requirement, so a future edit can never silently drop a floor
back under the gas the router needs on a staking-vault-underlying market.

These floors are FREE headroom (callers pay for gas USED, not the limit), so the
guard errs toward must-not-fail — a starved teardown remove strands on-chain
risk, the one thing teardown must never do.
"""

from almanak.connectors.pendle.sdk import (
    PENDLE_GAS_ESTIMATES,
    PENDLE_HEAVY_VAULT_SY_GAS_REQUIREMENTS,
)

# Minimum safety margin the floor must hold over the measured requirement. The
# heavy-vault-SY gas is near-constant (dominated by fixed blocklist/vault frames,
# not position size — verified 0.4 → 1000 sUSDai varied < 20 gas), so a 10%
# margin is a comfortable must-not-fail cushion on top of the chain buffer.
_MIN_MARGIN = 1.10


def test_heavy_vault_sy_floors_cover_measured_requirements():
    """Every heavy single-token op floor clears its measured requirement × margin."""
    for op, requirement in PENDLE_HEAVY_VAULT_SY_GAS_REQUIREMENTS.items():
        floor = PENDLE_GAS_ESTIMATES[op]
        needed = int(requirement * _MIN_MARGIN)
        assert floor >= needed, (
            f"Pendle gas floor for {op!r} is {floor}, below the measured "
            f"heavy-vault-SY requirement {requirement} × {_MIN_MARGIN} = {needed}. "
            f"On a staking-vault-underlying market (e.g. sUSDai) this risks the "
            f"OOG-in-SafeERC20 revert of VIB-5487. Raise the floor — do not lower "
            f"the requirement (it is on-chain-measured)."
        )


def test_remove_liquidity_floor_exceeds_bug_a_failure_limit():
    """The remove floor must exceed the exact limit that OOG'd the live tx.

    VIB-5487's failed tx ran with a 600k gas limit (before the chain buffer) and
    starved. Even ignoring the buffer, the raw floor must now clear the ~608k the
    remove actually needs.
    """
    assert PENDLE_GAS_ESTIMATES["remove_liquidity_single"] >= 608_000
    assert PENDLE_GAS_ESTIMATES["remove_liquidity_single"] > 600_000


def test_all_heavy_ops_present_in_requirement_table():
    """Guard the guard: the requirement table must not silently lose an op."""
    for op in PENDLE_HEAVY_VAULT_SY_GAS_REQUIREMENTS:
        assert op in PENDLE_GAS_ESTIMATES, f"{op} missing from PENDLE_GAS_ESTIMATES"
