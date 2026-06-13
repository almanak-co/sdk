"""StateManager delegation contract for the VIB-5059 quant-stats readers.

Mirrors the ``get_recent_snapshots`` passthrough pattern: no WARM backend,
an unsupported backend, or a failed read all degrade to the documented empty
values (zero-valued ``LedgerQuantStats`` / empty candidate list) — the same
inputs the legacy dashboard load produced from an empty ledger, so the tiles
render the honest empty state rather than erroring. The real-store happy
path is covered end-to-end in
``tests/unit/dashboard/test_quant_inputs_sql_equivalence.py``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.observability.ledger import LedgerEntry, LedgerQuantStats
from almanak.framework.state.state_manager import StateManager


def _make_manager(*, warm: object | None = None) -> StateManager:
    sm = StateManager.__new__(StateManager)
    sm._initialized = True
    sm._warm = warm
    sm._record_metrics = MagicMock()
    # __init__ owns this in production; the degrade paths warn through
    # _unimplemented_warn, which reads it.
    sm._unimplemented_logged = set()
    return sm


# =============================================================================
# get_ledger_quant_stats
# =============================================================================


@pytest.mark.asyncio
async def test_quant_stats_happy_path_delegates_to_warm() -> None:
    stats = LedgerQuantStats(total=7, gas_usd_sum=Decimal("1.23"))
    warm = MagicMock()
    warm.get_ledger_quant_stats = AsyncMock(return_value=stats)
    sm = _make_manager(warm=warm)

    assert await sm.get_ledger_quant_stats("dep-X") == stats
    warm.get_ledger_quant_stats.assert_awaited_once_with("dep-X")


@pytest.mark.asyncio
async def test_quant_stats_no_warm_backend_degrades_to_empty() -> None:
    sm = _make_manager(warm=None)
    assert await sm.get_ledger_quant_stats("dep-X") == LedgerQuantStats()


@pytest.mark.asyncio
async def test_quant_stats_unsupported_backend_degrades_to_empty() -> None:
    sm = _make_manager(warm=object())  # no get_ledger_quant_stats attribute
    assert await sm.get_ledger_quant_stats("dep-X") == LedgerQuantStats()


@pytest.mark.asyncio
async def test_quant_stats_backend_error_degrades_to_empty() -> None:
    warm = MagicMock()
    warm.get_ledger_quant_stats = AsyncMock(side_effect=RuntimeError("db down"))
    sm = _make_manager(warm=warm)

    assert await sm.get_ledger_quant_stats("dep-X") == LedgerQuantStats()


# =============================================================================
# get_ledger_anchor_candidates
# =============================================================================


@pytest.mark.asyncio
async def test_anchor_candidates_happy_path_delegates_with_bounds() -> None:
    rows = [LedgerEntry(deployment_id="dep-X")]
    warm = MagicMock()
    warm.get_ledger_anchor_candidates = AsyncMock(return_value=rows)
    sm = _make_manager(warm=warm)

    assert await sm.get_ledger_anchor_candidates("dep-X", limit=32, offset=64) == rows
    warm.get_ledger_anchor_candidates.assert_awaited_once_with("dep-X", limit=32, offset=64)


@pytest.mark.asyncio
async def test_anchor_candidates_no_warm_backend_degrades_to_empty() -> None:
    sm = _make_manager(warm=None)
    assert await sm.get_ledger_anchor_candidates("dep-X") == []


@pytest.mark.asyncio
async def test_anchor_candidates_unsupported_backend_degrades_to_empty() -> None:
    sm = _make_manager(warm=object())
    assert await sm.get_ledger_anchor_candidates("dep-X") == []


@pytest.mark.asyncio
async def test_anchor_candidates_backend_error_degrades_to_empty() -> None:
    warm = MagicMock()
    warm.get_ledger_anchor_candidates = AsyncMock(side_effect=RuntimeError("db down"))
    sm = _make_manager(warm=warm)

    assert await sm.get_ledger_anchor_candidates("dep-X") == []
