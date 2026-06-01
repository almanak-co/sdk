"""VIB-4879: boot-time WARNING when effective gwei cap is below typical gas.

The CI sanity test (``tests/unit/core/test_chain_gas_cap_sanity.py``)
catches chain-descriptor drift at merge time. This boot-time helper
catches the same condition at *runtime* — operators running a stale
SDK or a misconfigured chain-scoped override otherwise lose every
intent silently.

Contract:
1. Fires when ``effective_cap_gwei < typical * headroom_factor``
   (default 1.5x).
2. Once-per-process per chain.
3. Quarantined chains (no snapshot entry) are silently skipped.
4. Zero I/O.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.gas import constants as _gas_constants
from almanak.framework.execution.gas.constants import (
    OBSERVED_TYPICAL_GAS_GWEI,
    warn_if_effective_cap_below_typical_gas,
)


@pytest.fixture(autouse=True)
def _isolate_dedupe_state():
    """Snapshot + restore ``_EFFECTIVE_CAP_WARNED`` so tests that exercise it
    don't bleed into siblings sharing the same pytest-xdist worker."""
    original = set(_gas_constants._EFFECTIVE_CAP_WARNED)
    _gas_constants._EFFECTIVE_CAP_WARNED.clear()
    try:
        yield
    finally:
        _gas_constants._EFFECTIVE_CAP_WARNED.clear()
        _gas_constants._EFFECTIVE_CAP_WARNED.update(original)


def test_warns_when_cap_below_typical_gas_headroom() -> None:
    """Polygon typical ~284 gwei; a 200 cap is well below 1.5x = 425."""
    mock_logger = MagicMock()
    warn_if_effective_cap_below_typical_gas(chain="polygon", effective_cap_gwei=200, logger=mock_logger)
    assert mock_logger.warning.called
    msg = str(mock_logger.warning.call_args)
    assert "polygon" in msg
    assert "200" in msg
    assert "typical" in msg


def test_silent_when_cap_has_headroom() -> None:
    """Polygon at 1000 gwei (1000 / 284 = 3.5x) is healthy."""
    mock_logger = MagicMock()
    warn_if_effective_cap_below_typical_gas(chain="polygon", effective_cap_gwei=1000, logger=mock_logger)
    assert not mock_logger.warning.called


def test_silent_for_quarantined_chains() -> None:
    """Chains without a snapshot entry are silently skipped."""
    mock_logger = MagicMock()
    warn_if_effective_cap_below_typical_gas(chain="blast", effective_cap_gwei=1, logger=mock_logger)
    assert not mock_logger.warning.called


def test_dedupes_per_chain_per_process() -> None:
    """Repeated calls for the same chain emit the warning exactly once."""
    mock_logger = MagicMock()
    for _ in range(3):
        warn_if_effective_cap_below_typical_gas(chain="polygon", effective_cap_gwei=200, logger=mock_logger)
    assert mock_logger.warning.call_count == 1


def test_warns_per_chain_in_multi_chain_boot() -> None:
    """Different chains get their own warning slot."""
    mock_logger = MagicMock()
    # Pick two chains where 5 gwei is below 1.5x typical: polygon, mantle.
    for chain in ("polygon", "mantle"):
        warn_if_effective_cap_below_typical_gas(chain=chain, effective_cap_gwei=5, logger=mock_logger)
    assert mock_logger.warning.call_count == 2


@pytest.mark.parametrize("chain", sorted(OBSERVED_TYPICAL_GAS_GWEI.keys()))
def test_silent_at_2x_headroom_for_documented_chains(chain: str) -> None:
    """At 2x typical, every chain in the snapshot is healthy (this is the
    threshold the CI sanity test pins). Defends against future tweaks to
    the warning helper accidentally tightening the gate."""
    typical = OBSERVED_TYPICAL_GAS_GWEI[chain]
    # Cap = 2x typical (mirrors test_chain_gas_cap_sanity.py invariant).
    # max(2, ...) keeps the test honest for chains where typical is ~0.001 gwei.
    cap = max(2, int(typical * 2))
    mock_logger = MagicMock()
    warn_if_effective_cap_below_typical_gas(chain=chain, effective_cap_gwei=cap, logger=mock_logger)
    assert not mock_logger.warning.called, (
        f"Chain {chain!r}: a 2x-headroom cap should be silent. "
        f"typical={typical} gwei, cap={cap} gwei. "
        f"Warning was emitted: {mock_logger.warning.call_args}"
    )
