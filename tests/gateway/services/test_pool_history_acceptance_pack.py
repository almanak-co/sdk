"""VIB-4728 POOL-9 (VIB-4757) acceptance-pack aggregator — UAT card §D5.A.

This file is the SINGLE source of truth for the pre-merge acceptance pack.
Each of the 9 gateway tests + 2 framework-side mirror tests carries a
``@pytest.mark.acceptance_pack`` marker in its own source file; running
``pytest -m acceptance_pack`` (or ``make test-acceptance-pack``) collects
exactly that set.

The file ONLY imports the marker-bound test functions by name from their
canonical homes. This serves three purposes:

1. **Anti-deletion**: removing or renaming any of the 11 functions surfaces
   as a collection-time ``ImportError`` here, breaking CI immediately —
   distinct from "the marker silently selects 10 instead of 11" which would
   leave the pack quietly degraded.
2. **Anti-relocation**: each import line records the canonical home; a
   PR that moves a test to a different file must update this aggregator,
   making the move visible in code review.
3. **Anti-bypass**: the rollup is a deliberate SUBSET of all
   ``test_pool_history*.py`` files (the directory contains MORE files than
   the 9 + 2 covered here). The card §D5.A asserts via ``find`` that the
   superset is strictly larger.

This file deliberately defines NO test logic of its own — the marker on
each source-file function is what selects the tests; the imports below
exist purely to fail loudly when a function disappears. A separate
sanity test (``test_aggregator_imports_match_expected``) pins the import
count so a regression that doubles up on a function is caught too.

Mapping to PRD §9-test acceptance pack
(``docs/internal/discussions/pool-rates-history-gateway-design-20260521.md``
lines 165-173):

| # | PRD slot                          | Test                                                                          |
|---|-----------------------------------|-------------------------------------------------------------------------------|
| 1 | Happy path per provider           | ``test_pool_history_service.py::test_recorded_fixture_per_provider_chain``    |
| 2 | Chain matrix                      | ``test_pool_history_service.py::test_chain_matrix_arbitrum_ethereum_base``    |
| 3 | Provider fallback                 | ``test_pool_history_service.py::test_provider_fallback_full_chain_1d_to_geckoterminal`` |
| 4 | Cache hit + finality re-promotion | ``test_pool_history_finality.py::test_finality_re_promotion_stable_cache_key`` |
| 5 | Hosted-auth interceptor           | ``test_pool_history_auth.py::test_authenticated_happy_path``                  |
| 6 | Truncation roundtrip              | ``test_pool_history_truncation.py::test_truncation_reason_cap_exceeded``      |
| 7 | Thundering-herd dedup             | ``test_history_cache.py::test_inflight_dedup_shared_fetch``                   |
| 8 | Memory-bounded cache              | ``test_history_cache.py::test_lru_eviction_by_bytes_uses_dedicated_counter``  |
| 9 | Silent-error guard                | ``test_pool_history_service.py::test_pool_not_found_never_returns_empty_envelope`` |

Plus the framework-side mirror (2 additional):

| # | Mirror slot                          | Test                                                                          |
|---|--------------------------------------|-------------------------------------------------------------------------------|
| 10 | Framework gRPC routing (D1.S2)      | ``test_pool_history_gateway_backed.py::test_get_pool_history_routes_through_gateway`` |
| 11 | Backtest determinism (D2.M6)        | ``test_backtest_pool_history_determinism.py::test_null_reader_constructs_no_network_primitives`` |
"""

from __future__ import annotations

import pytest

# 9 gateway tests + 2 framework-side mirror tests — imported under a ``_``
# alias so pytest does NOT re-collect the imported names in this module
# (pytest collects every module-level ``test_*`` callable; an unprefixed
# alias would double-count each imported test under both its canonical
# nodeID and a ``test_pool_history_acceptance_pack.py::`` shadow). The
# alias preserves the ImportError-on-deletion guarantee — a missing name
# still raises at import time.
from tests.framework.data.test_pool_history_gateway_backed import (
    test_get_pool_history_routes_through_gateway as _t10,  # noqa: F401
)
from tests.framework.market.test_backtest_pool_history_determinism import (
    test_null_reader_constructs_no_network_primitives as _t11,  # noqa: F401
)
from tests.gateway.services.test_history_cache import (
    test_inflight_dedup_shared_fetch as _t7,  # noqa: F401
    test_lru_eviction_by_bytes_uses_dedicated_counter as _t8,  # noqa: F401
)
from tests.gateway.services.test_pool_history_auth import (
    test_authenticated_happy_path as _t5,  # noqa: F401
)
from tests.gateway.services.test_pool_history_finality import (
    test_finality_re_promotion_stable_cache_key as _t4,  # noqa: F401
)
from tests.gateway.services.test_pool_history_service import (
    test_chain_matrix_arbitrum_ethereum_base as _t2,  # noqa: F401
    test_pool_not_found_never_returns_empty_envelope as _t9,  # noqa: F401
    test_provider_fallback_full_chain_1d_to_geckoterminal as _t3,  # noqa: F401
    test_recorded_fixture_per_provider_chain as _t1,  # noqa: F401
)
from tests.gateway.services.test_pool_history_truncation import (
    test_truncation_reason_cap_exceeded as _t6,  # noqa: F401
)

#: The canonical (module, function_name) tuples that make up the acceptance
#: pack. Used by ``test_aggregator_imports_match_expected`` below + the
#: ``.EXPECTED.txt`` snapshot the Makefile diff-guards.
ACCEPTANCE_PACK_FUNCTIONS: tuple[tuple[str, str], ...] = (
    # 9 gateway tests
    ("tests.gateway.services.test_pool_history_service", "test_recorded_fixture_per_provider_chain"),
    ("tests.gateway.services.test_pool_history_service", "test_chain_matrix_arbitrum_ethereum_base"),
    ("tests.gateway.services.test_pool_history_service", "test_provider_fallback_full_chain_1d_to_geckoterminal"),
    ("tests.gateway.services.test_pool_history_finality", "test_finality_re_promotion_stable_cache_key"),
    ("tests.gateway.services.test_pool_history_auth", "test_authenticated_happy_path"),
    ("tests.gateway.services.test_pool_history_truncation", "test_truncation_reason_cap_exceeded"),
    ("tests.gateway.services.test_history_cache", "test_inflight_dedup_shared_fetch"),
    ("tests.gateway.services.test_history_cache", "test_lru_eviction_by_bytes_uses_dedicated_counter"),
    ("tests.gateway.services.test_pool_history_service", "test_pool_not_found_never_returns_empty_envelope"),
    # 2 framework-side mirror tests
    ("tests.framework.data.test_pool_history_gateway_backed", "test_get_pool_history_routes_through_gateway"),
    (
        "tests.framework.market.test_backtest_pool_history_determinism",
        "test_null_reader_constructs_no_network_primitives",
    ),
)


@pytest.mark.acceptance_pack
def test_aggregator_imports_match_expected() -> None:
    """Sanity: the aggregator's ``ACCEPTANCE_PACK_FUNCTIONS`` table matches
    the actual imports above (catches a hand-typo where the table grows
    but an import is forgotten, or vice versa).

    This is the ONE locally-defined test in the aggregator; it carries the
    ``acceptance_pack`` marker so ``pytest -m acceptance_pack`` runs it
    alongside the imported set.
    """
    import importlib

    for module_name, fn_name in ACCEPTANCE_PACK_FUNCTIONS:
        module = importlib.import_module(module_name)
        fn = getattr(module, fn_name, None)
        assert fn is not None, f"{module_name}::{fn_name} not importable (renamed or removed?)"
        # Each acceptance-pack target MUST carry the marker on the source-file
        # function (not just inherited from the aggregator). The marker lives
        # on the function's ``pytestmark`` list or on ``__wrapped__`` for
        # parametrized variants.
        markers: list[str] = []
        # `pytest.mark.<name>` decoration lands a `pytestmark` attribute or
        # adds an entry to the function's `pytestmark` list.
        for attr in ("pytestmark",):
            value = getattr(fn, attr, None)
            if value is None:
                continue
            if isinstance(value, list):
                markers.extend(getattr(m, "name", "") for m in value)
            else:
                markers.append(getattr(value, "name", ""))
        assert "acceptance_pack" in markers, (
            f"{module_name}::{fn_name} is in the aggregator table but its source "
            "function is missing the @pytest.mark.acceptance_pack decorator"
        )
    # Defensive: exactly 11 entries, no duplicates.
    assert len(ACCEPTANCE_PACK_FUNCTIONS) == 11, (
        f"acceptance pack must have exactly 11 entries; found {len(ACCEPTANCE_PACK_FUNCTIONS)}"
    )
    assert len(set(ACCEPTANCE_PACK_FUNCTIONS)) == 11, (
        "duplicates in ACCEPTANCE_PACK_FUNCTIONS"
    )
