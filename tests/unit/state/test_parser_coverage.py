"""Tier-1 parser-coverage regression test (VIB-4187 / T02 Hard Gate 1).

Companion to `docs/internal/qa/parser-coverage-audit-tier1-20260508.md`. The
audit doc is the headline deliverable; this test exists so the audit's
load-bearing claims do not silently rot:

1. The canonical taxonomy entries that drive the registry's `primitive` and
   `accounting_category` columns map each Tier-1 intent to the values the
   audit cites. If `record_for("LP_OPEN").primitive` ever stops being
   `Primitive.LP`, the audit's identity-tuple verdict (and T11's atomic
   commit primitive) is invalidated.
2. The Tier-1 receipt-parser classes and methods cited by the audit doc
   actually exist at the paths claimed. A renamed class or a removed
   `extract_lp_open_data` method would silently invalidate the audit's
   citations; this test catches that.

Heavier behavioral coverage (per-primitive identity-field corruption tests,
fork-variance parser-instantiation matrix, runtime mutation smoke test) is
explicitly deferred — the UAT card's D1.S3 / D2.S1 / D3.F1 sections describe
the full behavioral surface that follow-up tickets in the VIB-4185 epic
(notably T08 / VIB-4194 L1 offline goldens) will satisfy. This test ships
the structural floor below which the audit doc's claims cannot regress.

# TODO(VIB-4194 / T08): port the UAT card's full behavioral surface into
# this file (or a sibling) once `tests/fixtures/parser-coverage/` ships:
#   - D1.S3 mutation smoke test (monkey-patch parser identity emit; assert
#     the suite FAILs under mutation — catches tautological-test patterns).
#   - D2.S1 fork-variance parser-instantiation matrix (UniV3 vs Sushi V3 vs
#     PancakeSwap V3 vs Slipstream — assert each fork's manager address
#     filters logs correctly and emits the same identity tuple shape).
#   - D2.S2 GMX is_long boolean coverage (long/short fixtures — assert
#     parser emits the boolean both ways, not just one direction).
#   - D3.F1 corruption tests (per receipt-derivable identity field, mutate
#     the topic/data byte and assert parser raises or returns None — does
#     NOT silently emit a defaulted typed record). The audit's "Reachable
#     defensive defaults" caveat in the cross-cutting section enumerates
#     the GMX V2 + Pendle defensive-default sites that T08 must cover.
"""

from __future__ import annotations

import importlib

import pytest

from almanak.framework.primitives.taxonomy import record_for
from almanak.framework.primitives.types import (
    AccountingCategory,
    Primitive,
)

# Canonical mapping — the audit doc's per-primitive verdicts depend on each row
# resolving exactly as listed. Round 10 of Phase 1 critique pinned this set
# (REQUIRED_IDENTITY_FIELDS in the UAT card) so the registry cannot silently
# misclassify Tier-1 intents.
EXPECTED_RECORDS: dict[str, tuple[Primitive, AccountingCategory]] = {
    "LP_OPEN": (Primitive.LP, AccountingCategory.LP),
    "LP_CLOSE": (Primitive.LP, AccountingCategory.LP),
    "PERP_OPEN": (Primitive.PERP, AccountingCategory.PERP),
    "PERP_CLOSE": (Primitive.PERP, AccountingCategory.PERP),
    "PENDLE_LP_OPEN": (Primitive.LP, AccountingCategory.PENDLE_LP),
    "PENDLE_LP_CLOSE": (Primitive.LP, AccountingCategory.PENDLE_LP),
}


# D2.S4: each Tier-1 intent gets its OWN per-string assertion against the
# EXACT (Primitive, AccountingCategory) tuple — not a global "any Primitive.X"
# regex. A buggy taxonomy entry (e.g. PENDLE_LP_OPEN routed to AccountingCategory.LP
# instead of PENDLE_LP) is caught here, not silently registered as a UniV3 LP.
def test_record_for_lp_open():
    expected = EXPECTED_RECORDS["LP_OPEN"]
    assert record_for("LP_OPEN").primitive == Primitive.LP
    assert record_for("LP_OPEN").accounting_category == AccountingCategory.LP
    assert (record_for("LP_OPEN").primitive, record_for("LP_OPEN").accounting_category) == expected


def test_record_for_lp_close():
    expected = EXPECTED_RECORDS["LP_CLOSE"]
    assert record_for("LP_CLOSE").primitive == Primitive.LP
    assert record_for("LP_CLOSE").accounting_category == AccountingCategory.LP
    assert (record_for("LP_CLOSE").primitive, record_for("LP_CLOSE").accounting_category) == expected


def test_record_for_perp_open():
    expected = EXPECTED_RECORDS["PERP_OPEN"]
    assert record_for("PERP_OPEN").primitive == Primitive.PERP
    assert record_for("PERP_OPEN").accounting_category == AccountingCategory.PERP
    assert (record_for("PERP_OPEN").primitive, record_for("PERP_OPEN").accounting_category) == expected


def test_record_for_perp_close():
    expected = EXPECTED_RECORDS["PERP_CLOSE"]
    assert record_for("PERP_CLOSE").primitive == Primitive.PERP
    assert record_for("PERP_CLOSE").accounting_category == AccountingCategory.PERP
    assert (record_for("PERP_CLOSE").primitive, record_for("PERP_CLOSE").accounting_category) == expected


def test_record_for_pendle_lp_open():
    expected = EXPECTED_RECORDS["PENDLE_LP_OPEN"]
    assert record_for("PENDLE_LP_OPEN").primitive == Primitive.LP
    assert record_for("PENDLE_LP_OPEN").accounting_category == AccountingCategory.PENDLE_LP
    assert (record_for("PENDLE_LP_OPEN").primitive, record_for("PENDLE_LP_OPEN").accounting_category) == expected


def test_record_for_pendle_lp_close():
    expected = EXPECTED_RECORDS["PENDLE_LP_CLOSE"]
    assert record_for("PENDLE_LP_CLOSE").primitive == Primitive.LP
    assert record_for("PENDLE_LP_CLOSE").accounting_category == AccountingCategory.PENDLE_LP
    assert (record_for("PENDLE_LP_CLOSE").primitive, record_for("PENDLE_LP_CLOSE").accounting_category) == expected


# Audit-doc citation invariant: the parser classes and methods the audit cites
# must exist. If a parser is renamed or a method removed, the audit's file:line
# citations rot silently — this test surfaces that as a unit-test failure.
PARSER_CITATIONS = [
    # (module, class candidates, method candidates) — accept any of the listed
    # class names so the test survives benign renames; require at least one
    # method per class.
    (
        "almanak.framework.connectors.uniswap_v3.receipt_parser",
        ["UniswapV3ReceiptParser"],
        ["extract_lp_open_data", "extract_lp_close_data"],
    ),
    (
        "almanak.framework.connectors.gmx_v2.receipt_parser",
        ["GMXv2ReceiptParser"],
        ["_parse_position_increase", "_parse_position_decrease"],
    ),
    (
        "almanak.framework.connectors.pendle.receipt_parser",
        ["PendleReceiptParser", "PendleLPReceiptParser"],
        ["extract_lp_open_data", "extract_lp_close_data"],
    ),
]


def _parser_id(module_path: str) -> str:
    """Pick a stable, human-readable test id from the parser module path.

    Uses the connector name segment when available (e.g. ``uniswap_v3`` from
    ``almanak.framework.connectors.uniswap_v3.receipt_parser``); falls back to
    the leaf module name when the path is shallower than expected. Avoids the
    ``rsplit(".", 2)[-2]`` IndexError trap on shallow paths (gemini-bot, PR
    #2200 review).
    """
    parts = module_path.split(".")
    return parts[-2] if len(parts) >= 2 else module_path


@pytest.mark.parametrize(
    "module_path,class_names,method_names",
    PARSER_CITATIONS,
    ids=[_parser_id(c[0]) for c in PARSER_CITATIONS],
)
def test_parser_class_and_methods_exist(module_path, class_names, method_names):
    """Each Tier-1 receipt parser must expose the audit-cited surface."""
    module = importlib.import_module(module_path)
    found_class = None
    for cls_name in class_names:
        if hasattr(module, cls_name):
            found_class = getattr(module, cls_name)
            break
    assert found_class is not None, (
        f"audit cites a class in {module_path} (any of {class_names}); "
        f"none found — parser may have been renamed"
    )
    missing = [m for m in method_names if not hasattr(found_class, m)]
    assert not missing, (
        f"{found_class.__name__} is missing audit-cited methods: {missing}. "
        f"Audit doc citations are stale — refresh "
        f"docs/internal/qa/parser-coverage-audit-tier1-20260508.md"
    )


# Pendle expiry-source citation: the audit's Pendle reconciliation says
# `expiry_ts` lives on `on_chain_reader.py:304` via `_gateway_eth_call` keyed by
# `f"expiry:{market_address.lower()}"`. If that path moves or the cache key
# shape changes, the audit's "expiry is sourced via on_chain_reader, NOT via
# the receipt parser" claim is invalidated and Pendle's TIER-1-CLEAN-WITH-CAVEAT
# verdict needs revisiting.
def test_pendle_expiry_eth_call_path_exists():
    module = importlib.import_module("almanak.framework.data.pendle.on_chain_reader")
    assert hasattr(module, "EXPIRY_SELECTOR"), (
        "audit cites EXPIRY_SELECTOR in pendle/on_chain_reader.py; "
        "constant has been renamed or removed"
    )
    assert module.EXPIRY_SELECTOR.lower() == "0xe184c9be", (
        f"EXPIRY_SELECTOR ({module.EXPIRY_SELECTOR}) does not match the "
        f"`expiry()` method selector — audit citation invalidated"
    )
