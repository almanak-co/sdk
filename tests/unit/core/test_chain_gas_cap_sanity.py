"""VIB-4879: pin chain-descriptor gas caps against observed live gas.

`GasProfile.price_cap_gwei` is the per-chain hard ceiling: a transaction whose
effective gwei exceeds this cap is rejected by `_validate_gas_prices` in the
execution orchestrator. When live network gas drifts above the descriptor's
cap, every intent on that chain is silently blocked — exactly the bug that
shipped on Mantle (cap=10, live=50) and the user-reported symptom that opened
this ticket on Polygon (the env override there was the proximate cause, but
the descriptor would have failed too once env-override was removed if it had
been too tight).

These tests pin `descriptor.price_cap_gwei >= 2 × observed_typical_gas` for
every chain we have a live snapshot for. The 2× factor is the minimum spike
headroom we ship — anything tighter requires explicit justification per chain
and an updated snapshot in the table below.

Snapshot date: 2026-05-27/28 (PR #2476 investigation, multi-RPC sweep).
Refresh policy: when a chain's `price_cap_gwei` is changed, the snapshot row
that justifies it must be updated alongside the change. Quarantined chains
(no observable gas at snapshot time) are documented separately so the test
breaks loudly if we add a cap for them without a snapshot.
"""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.framework.execution.gas.constants import (
    OBSERVED_TYPICAL_GAS_GWEI,
)
from almanak.framework.execution.gas.constants import (
    SANE_GWEI_CEILING as _PROD_SANE_GWEI_CEILING,
)

# Chains with no observable gas at snapshot time (RPC unreachable, no canonical
# public endpoint, etc.). Keep them OUT of the strict assertion to avoid
# breaking the test on infra flakes — but the registry IS still expected to
# carry a sane cap for them; the test only requires it not exceed the
# SANE_GWEI_CEILING used by the USD→gwei conversion path.
GAS_CAP_QUARANTINE: set[str] = {"blast", "monad"}

# Pull the absolute ceiling from production constants so the test cannot
# drift from the value the gas guard actually uses.
SANE_GWEI_CEILING: int = _PROD_SANE_GWEI_CEILING


@pytest.mark.parametrize(
    ("chain_name", "observed_gwei"),
    sorted(OBSERVED_TYPICAL_GAS_GWEI.items()),
)
def test_chain_price_cap_has_2x_headroom_over_observed_gas(chain_name: str, observed_gwei: float) -> None:
    """Per-chain `price_cap_gwei` must give >=2x headroom over observed gas."""
    descriptor = ChainRegistry.try_resolve(chain_name)
    assert descriptor is not None, f"Chain {chain_name!r} not registered"
    cap = descriptor.gas.price_cap_gwei
    assert cap is not None, f"Chain {chain_name!r} has no price_cap_gwei set"

    required_min = observed_gwei * 2
    assert cap >= required_min, (
        f"Chain {chain_name!r} price_cap_gwei={cap} but observed live gas was "
        f"~{observed_gwei} gwei (needs >= {required_min:.1f} for 2x spike "
        f"headroom). Either re-snapshot live gas and update "
        f"OBSERVED_TYPICAL_GAS_GWEI, or bump the descriptor's price_cap_gwei "
        f"in almanak/core/chains/{chain_name}.py."
    )


@pytest.mark.parametrize("chain_name", sorted(OBSERVED_TYPICAL_GAS_GWEI.keys() | GAS_CAP_QUARANTINE))
def test_chain_price_cap_stays_below_sane_ceiling(chain_name: str) -> None:
    """Every registered chain's cap must stay below the SANE_GWEI_CEILING.

    The USD-cost cap path converts USD → gwei using the current native price;
    if a descriptor's cap (which is the fallback when the oracle has no price)
    crept above the ceiling, the fallback would become looser than the USD
    intent. SANE_GWEI_CEILING is the hard product invariant.
    """
    descriptor = ChainRegistry.try_resolve(chain_name)
    if descriptor is None:
        pytest.skip(f"Chain {chain_name!r} not registered yet")

    cap = descriptor.gas.price_cap_gwei
    if cap is None:
        pytest.skip(f"Chain {chain_name!r} has no price_cap_gwei (falls back to DEFAULT_GAS_PRICE_CAP_GWEI)")

    assert cap <= SANE_GWEI_CEILING, (
        f"Chain {chain_name!r} price_cap_gwei={cap} exceeds SANE_GWEI_CEILING "
        f"({SANE_GWEI_CEILING}). The USD-cost cap path uses this ceiling to "
        f"clamp implicit gwei when native price is near zero; a chain cap "
        f"above it breaks that invariant."
    )


# =============================================================================
# VIB-5673: the tip floor must stay anchored to observed base fee
# =============================================================================

# The tip floor may not exceed `max(TIP_FLOOR_ABSOLUTE_ALLOWANCE_GWEI,
# TIP_FLOOR_MAX_BASE_FEE_MULTIPLE * observed_typical_base_fee)`.
#
# Why two terms:
#   * The multiple is the real guard. The VIB-5673 regression was ethereum's
#     floor sitting at 12.5x the observed base fee (2.0 gwei vs 0.16), making
#     the tip 86% of max_fee. Since the tip is ALWAYS paid under EIP-1559,
#     that is a direct ~10x overpay on every transaction.
#   * The absolute allowance stops the multiple from being absurdly strict on
#     chains whose base fee is near zero (optimism and berachain snapshot at
#     0.001 gwei; 3x of that is 0.003 gwei). A tip at or below this allowance
#     costs ~$0.02 on a 400k-gas tx regardless of chain, so it cannot be an
#     overpay worth failing CI over.
TIP_FLOOR_MAX_BASE_FEE_MULTIPLE: float = 3.0
TIP_FLOOR_ABSOLUTE_ALLOWANCE_GWEI: float = 0.05


def _assert_tip_floor_anchored(chain_name: str, floor: float, observed_gwei: float) -> None:
    """Assert one chain's tip floor is anchored to its observed base fee.

    Extracted from the parametrized test so the **failure-message path** is
    directly testable. That path is only executed when the gate fires, so a
    bug in it (e.g. a ZeroDivisionError formatting the ratio) would otherwise
    stay invisible until the one moment the gate is needed — and a gate whose
    failure message crashes tells the operator nothing.
    """
    allowed = max(
        TIP_FLOOR_ABSOLUTE_ALLOWANCE_GWEI,
        TIP_FLOOR_MAX_BASE_FEE_MULTIPLE * observed_gwei,
    )
    # Guard the ratio: observed_gwei can legitimately be 0.0 (a chain whose
    # base fee rounds to zero at snapshot time). The ratio is only a
    # human-readable aid; it must never mask the actual assertion failure.
    ratio = f"{floor / observed_gwei:.1f}x" if observed_gwei else "N/Ax (observed base fee is 0)"
    assert floor <= allowed, (
        f"Chain {chain_name!r} min_priority_fee_gwei={floor} but observed "
        f"typical base fee is ~{observed_gwei} gwei — the tip floor is "
        f"{ratio} base fee (max allowed: {allowed:.3f} "
        f"gwei = max({TIP_FLOOR_ABSOLUTE_ALLOWANCE_GWEI}, "
        f"{TIP_FLOOR_MAX_BASE_FEE_MULTIPLE}x{observed_gwei}). The tip is "
        f"ALWAYS paid under EIP-1559, so a floor above base fee overpays on "
        f"every transaction (VIB-5673). Re-tune "
        f"almanak/core/chains/{chain_name}.py, or re-snapshot "
        f"OBSERVED_TYPICAL_GAS_GWEI if base fees genuinely moved."
    )


def test_tip_floor_gate_reports_cleanly_at_zero_observed_base_fee() -> None:
    """VIB-5673: the gate must FAIL LOUDLY, not crash, when observed base is 0.

    `observed_gwei == 0.0` is reachable — optimism is already snapshotted at
    0.001 gwei and trending down, so a future re-snapshot could round it to
    zero. At 0.0 the allowance collapses to the absolute term (0.05 gwei), so
    any real floor above that SHOULD trip the gate — and that is exactly when
    the message formats the `floor / observed_gwei` ratio.

    Without the guard this raises ZeroDivisionError instead of AssertionError:
    the gate still stops the merge, but reports a crash rather than the reason,
    which is unactionable. Pin the clean-failure behaviour.
    """
    with pytest.raises(AssertionError) as exc_info:
        _assert_tip_floor_anchored("fake-zero-base-chain", floor=2.0, observed_gwei=0.0)

    message = str(exc_info.value)
    # The diagnosis must survive: which chain, what value, and what to do.
    assert "fake-zero-base-chain" in message
    assert "min_priority_fee_gwei=2.0" in message
    assert "N/Ax" in message  # ratio degraded gracefully, not crashed
    assert "VIB-5673" in message


def test_tip_floor_gate_passes_at_zero_observed_base_fee_for_negligible_floor() -> None:
    """A negligible floor must still pass when observed base fee is 0.

    The absolute allowance (0.05 gwei ~ $0.02 on a 400k-gas tx) is what keeps
    the multiple from being infinitely strict on a zero-base chain.
    """
    _assert_tip_floor_anchored("fake-zero-base-chain", floor=0.02, observed_gwei=0.0)


@pytest.mark.parametrize(
    ("chain_name", "observed_gwei"),
    sorted(OBSERVED_TYPICAL_GAS_GWEI.items()),
)
def test_tip_floor_is_not_far_above_observed_base_fee(chain_name: str, observed_gwei: float) -> None:
    """VIB-5673: `min_priority_fee_gwei` must stay anchored to live base fee.

    An absolute gwei tip floor rots silently: ethereum's 2.0 gwei was a
    sensible ~5% tip when base fees were 20-50 gwei, but post-blob L1 sits at
    ~0.16 gwei, leaving the floor at 12.5x base. Nothing failed — the floor
    just quietly became 86% of every L1 transaction's gas price, overrode the
    node's own landable ~0.05 gwei suggestion, and inflated the node's
    `balance >= gas_limit * maxFeePerGas + value` admission check enough to
    reject well-funded wallets with -32003.

    This gate makes that drift a merge-time failure instead of a live
    regression. If it fires, the fix is normally to re-tune the descriptor
    (and/or re-snapshot OBSERVED_TYPICAL_GAS_GWEI if base fees genuinely
    moved) — NOT to loosen the constants above.

    Exception: a genuinely protocol-enforced minimum (polygon's validator-gated
    ~30 gwei) legitimately has whatever value the protocol demands. Polygon
    passes comfortably here (30 vs 3 x 283.95); if a future chain has a hard
    minimum that trips this gate, document it and quarantine it explicitly
    rather than raising the multiple for everyone.
    """
    descriptor = ChainRegistry.try_resolve(chain_name)
    assert descriptor is not None, f"Chain {chain_name!r} not registered"

    floor = descriptor.gas.min_priority_fee_gwei
    if not floor:
        # None / 0.0 → chain declares no floor policy (the L2 case). Nothing
        # to anchor; `priority_fee_floor_wei` returns 0 and the node's own
        # suggestion is used verbatim.
        return

    _assert_tip_floor_anchored(chain_name, floor, observed_gwei)


def test_mantle_specific_cap_after_vib_4879_bump() -> None:
    """Mantle was 10 gwei pre-VIB-4879 with live ~50 gwei (blocked everything).

    Pin the bumped value explicitly so a future revert is loud.
    """
    descriptor = ChainRegistry.try_resolve("mantle")
    assert descriptor is not None
    assert descriptor.gas.price_cap_gwei >= 100, (
        "Mantle price_cap_gwei must remain >= 100 (VIB-4879 minimum). "
        "Pre-bump value of 10 blocked every Mantle intent regardless of any "
        "env override."
    )


def test_sonic_specific_cap_after_vib_4879_bump() -> None:
    """Sonic was 100 gwei pre-VIB-4879 with live ~55 gwei (1.8x headroom).

    Pin the bumped value to keep spike headroom.
    """
    descriptor = ChainRegistry.try_resolve("sonic")
    assert descriptor is not None
    assert descriptor.gas.price_cap_gwei >= 200, "Sonic price_cap_gwei must remain >= 200 (VIB-4879 minimum)."
