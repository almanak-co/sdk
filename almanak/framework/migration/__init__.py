"""Per-primitive cutover migration to ``position_registry`` (VIB-4185 / T04).

This package is the home of the multi-position-tracking epic's per-primitive
cutover infrastructure. The shared playbook lives at
``docs/internal/migration-cutover-position-registry.md`` and the design at
``docs/internal/blueprints/28-position-registry.md`` §5.

The shared invariants — idempotent backfill, OPEN/CLOSE fold, boot-guard
sequencing, and structured ``BackfillReport`` / ``MigrationStateRow``
dataclasses — live in :mod:`backfill`. Each per-primitive cutover ticket
contributes a concrete subclass (``UniV3LPCutoverReader``, etc.) that
implements the receipt-fact-only identity tuple and per-primitive payload
extraction hooks.

T12 (VIB-4198) ships the UniV3 LP cutover; T16 / T23 / T28 ship the GMX V2,
Pendle LP, and Aave V3 cutovers respectively. Each of them subclasses the
same base reader and reuses the same boot-guard.

This module is **production code**. The static anti-bypass guard
(``tests/static/test_open_position_query_outside_registry.py``) explicitly
allowlists this directory as the **only** legitimate code path under
``almanak/framework/`` that may read ``position_events`` for routing
purposes — backfill IS the migration, by definition.
"""

from almanak.framework.migration.backfill import (
    BackfillFailedError,
    BackfillReader,
    BackfillReport,
    CutoverStorageNotSupported,
    LendingCutoverReader,
    MigrationStateRow,
    PerpCutoverReader,
    RegistryBackfillIncompleteError,
    RegistryCutoverNotDeployedError,
    RegistryLookupInstallError,
    fold_position_events_for_lending,
    fold_position_events_for_perp,
    fold_position_events_for_univ3,
    fold_position_events_for_univ4,
    lending_leg_for_position_type,
    lending_registry_market_id,
    perp_direction_label,
    physical_identity_hash_lending,
    physical_identity_hash_perp,
    physical_identity_hash_univ3,
    physical_identity_hash_univ4,
    semantic_grouping_key_lending,
    semantic_grouping_key_perp,
    semantic_grouping_key_univ3,
    semantic_grouping_key_univ4,
)

__all__ = [
    "BackfillFailedError",
    "BackfillReader",
    "BackfillReport",
    "CutoverStorageNotSupported",
    "LendingCutoverReader",
    "MigrationStateRow",
    "PerpCutoverReader",
    "RegistryBackfillIncompleteError",
    "RegistryCutoverNotDeployedError",
    "RegistryLookupInstallError",
    "fold_position_events_for_lending",
    "fold_position_events_for_perp",
    "fold_position_events_for_univ3",
    "fold_position_events_for_univ4",
    "lending_leg_for_position_type",
    "lending_registry_market_id",
    "perp_direction_label",
    "physical_identity_hash_lending",
    "physical_identity_hash_perp",
    "physical_identity_hash_univ3",
    "physical_identity_hash_univ4",
    "semantic_grouping_key_lending",
    "semantic_grouping_key_perp",
    "semantic_grouping_key_univ3",
    "semantic_grouping_key_univ4",
]
