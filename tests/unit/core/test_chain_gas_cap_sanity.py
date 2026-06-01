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
