"""Freeze the pre-T2 routing truth tables (VIB-4162 precursor).

Run ONCE on the precursor commit to freeze the routing decisions of the
four consumers being migrated. The post-T2 parity tests load these JSON
files and assert delegation parity.

Three artifacts are produced:

* ``legacy_classifier_truth_table.json`` — cartesian product of every
  IntentType value × representative protocols × representative tokens,
  recording the canonical ``primitives.taxonomy.classify(...)`` value.
  The taxonomy is the **single source of truth** at precursor time; the
  pre-T2 ``accounting.classifier.classify`` agrees with it on every
  IntentType already routed, and on the small set of T1-introduced
  additions (VAULT_MANAGE, VAULT_REALLOCATE) the taxonomy is the
  authoritative answer (the pre-T2 frozenset-based classifier silently
  defaulted to NO_ACCOUNTING for those — preserving that quirk in the
  truth table would freeze a known bug). T2 then deletes the local
  classifier and re-points consumers at the taxonomy; the parity test
  verifies the consumer-side ``classify`` returns the locked taxonomy
  value for every triple.
* ``legacy_position_type_truth_table.json`` — for every IntentType,
  the pre-T2 ``observability.position_events.INTENT_TO_POSITION_TYPE``
  value (using ``.get`` with NO default — the silent-LP fallback that's
  the bug T2 fixes).
* ``legacy_lifecycle_truth_table.json`` — for every IntentType, the
  ``required_lifecycle`` declared by T1's taxonomy.

The output JSON is deterministic: rows are sorted by (intent, protocol,
token_out) so the file is byte-stable across runs.
"""

from __future__ import annotations

import json
from pathlib import Path

from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.primitives.taxonomy import (
    TAXONOMY,
    _resolve_alias,
    classify,
    position_type_for,
)

PROTOCOLS = (
    "",
    "uniswap_v3",
    "aerodrome",
    "pendle_v2",
    "aave_v3",
    "morpho_blue",
    "compound_v3",
    "gmx_v2",
    "kamino",
    "jupiter",
)
TOKENS = ("", "PT-stETH", "USDC", "WETH")


def _intent_values() -> list[str]:
    """Sorted list of every IntentType value declared in vocabulary.py."""
    return sorted({m.value for m in IntentType})


def freeze_classifier_truth_table(out_path: Path) -> None:
    rows: list[dict] = []
    for intent in _intent_values():
        for protocol in PROTOCOLS:
            for token_out in TOKENS:
                category = classify(intent, protocol, token_out)
                rows.append(
                    {
                        "intent": intent,
                        "protocol": protocol,
                        "token_out": token_out,
                        "expected": {"category": category.value},
                    }
                )
    rows.sort(key=lambda r: (r["intent"], r["protocol"], r["token_out"]))
    out_path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")


def freeze_position_type_truth_table(out_path: Path) -> None:
    """Record taxonomy.position_type_for for every IntentType.

    The pre-T2 ``INTENT_TO_POSITION_TYPE`` dict only carried entries for
    LP / PERP / SUPPLY / BORROW / REPAY / WITHDRAW / DELEVERAGE; every
    other IntentType returned PositionType.LP via the silent-LP fallback
    that T2 exists to fix. The post-T2 contract is "intents that are not
    position-producing return None"; the taxonomy is the canonical
    source. We freeze taxonomy values (filtered to the intents that pre-T2
    actually emitted position events for, plus ``None`` for the rest)
    so the parity test verifies the consumer-side resolver returns the
    same values for known intents and None for the rest.
    """
    # The pre-T2 explicit mapping (kept here as the parity floor: any
    # intent listed here MUST resolve to the same PositionType post-T2).
    legacy_mapping = {
        "LP_OPEN": "LP",
        "LP_CLOSE": "LP",
        "LP_COLLECT_FEES": "LP",
        "PERP_OPEN": "PERP",
        "PERP_CLOSE": "PERP",
        "SUPPLY": "LENDING_COLLATERAL",
        "WITHDRAW": "LENDING_COLLATERAL",
        "BORROW": "LENDING_DEBT",
        "REPAY": "LENDING_DEBT",
        "DELEVERAGE": "LENDING_DEBT",
    }
    rows: list[dict] = []
    for intent in _intent_values():
        rows.append(
            {
                "intent": intent,
                "expected": {"position_type": legacy_mapping.get(intent)},
            }
        )
    rows.sort(key=lambda r: r["intent"])
    out_path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")


def freeze_lifecycle_truth_table(out_path: Path) -> None:
    rows: list[dict] = []
    for intent in _intent_values():
        record = TAXONOMY.get(_resolve_alias(intent))
        lifecycle = list(record.required_lifecycle) if record is not None else []
        rows.append(
            {
                "intent": intent,
                "expected": {"lifecycle": lifecycle},
            }
        )
    rows.sort(key=lambda r: r["intent"])
    out_path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")


def main() -> None:
    base = Path(__file__).parent
    freeze_classifier_truth_table(base / "legacy_classifier_truth_table.json")
    freeze_position_type_truth_table(base / "legacy_position_type_truth_table.json")
    freeze_lifecycle_truth_table(base / "legacy_lifecycle_truth_table.json")


if __name__ == "__main__":
    main()
