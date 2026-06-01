"""POOL-6 (VIB-4754) settings validators + dispatcher cutoff defaults.

The per-provider finality-cutoff and page-cap-rows validators are gateway-
perimeter SAFETY guards: a typo (``...CUTOFF_SECONDS_DEFILLAMA=0`` or
``...PAGE_CAP_ROWS_THE_GRAPH=0``) must NOT silently mark revisable data finalized
or truncate every response to zero rows — it must fall back to the field default.
These tests exercise the non-positive / malformed fallback paths the happy-path
truncation tests don't (pr-auditor Important #3), and confirm the dispatcher's
provider-specific cutoff defaults survive an omitted ``finality_cutoffs``
(CodeRabbit: DefiLlama must default to 72h, not a flat 24h).
"""

from __future__ import annotations

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.data.pool_history import PoolHistoryDispatcher

_BAD_VALUES = [0, -1, "abc", ""]


@pytest.mark.parametrize("bad", _BAD_VALUES)
def test_finality_cutoff_invalid_falls_back_to_default(bad: object) -> None:
    s = GatewaySettings(
        pool_history_finality_cutoff_seconds_the_graph=bad,
        pool_history_finality_cutoff_seconds_defillama=bad,
        pool_history_finality_cutoff_seconds_geckoterminal=bad,
    )
    assert s.pool_history_finality_cutoff_seconds_the_graph == 86400
    assert s.pool_history_finality_cutoff_seconds_defillama == 259200
    assert s.pool_history_finality_cutoff_seconds_geckoterminal == 86400


def test_finality_cutoff_valid_override_kept() -> None:
    s = GatewaySettings(pool_history_finality_cutoff_seconds_defillama=100000)
    assert s.pool_history_finality_cutoff_seconds_defillama == 100000


@pytest.mark.parametrize("bad", _BAD_VALUES)
def test_page_cap_rows_invalid_falls_back_to_default(bad: object) -> None:
    s = GatewaySettings(
        pool_history_page_cap_rows_the_graph=bad,
        pool_history_page_cap_rows_defillama=bad,
        pool_history_page_cap_rows_geckoterminal=bad,
    )
    assert s.pool_history_page_cap_rows_the_graph == 100000
    assert s.pool_history_page_cap_rows_defillama == 100000
    assert s.pool_history_page_cap_rows_geckoterminal == 100000


def test_page_cap_rows_valid_override_kept() -> None:
    s = GatewaySettings(pool_history_page_cap_rows_the_graph=100)
    assert s.pool_history_page_cap_rows_the_graph == 100


def test_dispatcher_finality_cutoffs_default_to_provider_specific() -> None:
    # A direct dispatcher with no finality_cutoffs must still classify DefiLlama
    # with the 72h contract — NOT a flat 24h (CodeRabbit).
    d = PoolHistoryDispatcher(
        thegraph_api_key=None,
        thegraph_monthly_budget_max=100000,
        is_supported_fn=lambda _c, _p: True,
    )
    assert d._finality_cutoffs["the_graph"] == 86400
    assert d._finality_cutoffs["defillama"] == 259200
    assert d._finality_cutoffs["geckoterminal"] == 86400


def test_dispatcher_finality_cutoffs_override_merges_over_defaults() -> None:
    d = PoolHistoryDispatcher(
        thegraph_api_key=None,
        thegraph_monthly_budget_max=100000,
        is_supported_fn=lambda _c, _p: True,
        finality_cutoffs={"the_graph": 999},
    )
    assert d._finality_cutoffs["the_graph"] == 999  # override applied
    assert d._finality_cutoffs["defillama"] == 259200  # default preserved
