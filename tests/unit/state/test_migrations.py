"""Unit tests for state schema migration application (`migrate`).

Covers the sequential-apply contract of
``almanak.framework.state.migrations.migrate``: no-op short-circuit,
target-version defaulting, missing-migration failure, sequential success,
and stop-on-first-failure semantics (state preserved at the failure point).

Every test builds its own ``MigrationRegistry`` — the global registry is
shared process state and must not be mutated from tests.
"""

from almanak.framework.state.migrations import (
    MigrationRegistry,
    StateMigration,
    migrate,
)


def _add_field_v2(state: dict) -> dict:
    state["field_x"] = "v2-default"
    return state


def _rename_field_v3(state: dict) -> dict:
    state["field_y"] = state.pop("field_x")
    return state


def _registry_with(*migrations: StateMigration) -> MigrationRegistry:
    reg = MigrationRegistry()
    for m in migrations:
        reg.register(m)
    return reg


class TestMigrateNoOp:
    def test_same_version_returns_success_without_touching_state(self):
        state = {"a": 1}

        result = migrate(state, from_version=3, to_version=3)

        assert result.success is True
        assert result.from_version == 3
        assert result.to_version == 3
        assert result.migrations_applied == []
        assert result.state is state  # untouched, same object
        assert result.duration_ms == 0.0

    def test_downgrade_request_is_a_noop_reporting_from_version(self):
        """from > to is not a rollback — migrate() reports staying at from."""
        state = {"a": 1}

        result = migrate(state, from_version=5, to_version=2)

        assert result.success is True
        assert result.to_version == 5
        assert result.migrations_applied == []
        assert result.state is state


class TestMigrateTargetDefaulting:
    def test_to_version_defaults_to_registry_current_version(self):
        reg = _registry_with(
            StateMigration(version=2, migration_fn=_add_field_v2),
            StateMigration(version=3, migration_fn=_rename_field_v3),
        )

        result = migrate({"a": 1}, from_version=1, registry=reg)

        assert result.success is True
        assert result.to_version == 3
        assert result.migrations_applied == [2, 3]
        assert result.state == {"a": 1, "field_y": "v2-default"}


class TestMigrateMissingMigration:
    def test_gap_in_migration_chain_fails_without_applying_anything(self):
        # v3 registered but v2 missing: the 1 -> 3 path cannot be built.
        reg = _registry_with(StateMigration(version=3, migration_fn=_rename_field_v3))
        state = {"a": 1}

        result = migrate(state, from_version=1, to_version=3, registry=reg)

        assert result.success is False
        assert result.migrations_applied == []
        assert result.state is state  # untouched
        assert result.error is not None
        assert "version 2" in result.error
        assert result.from_version == 1
        assert result.to_version == 3


class TestMigrateSequentialApply:
    def test_applies_migrations_in_order_and_reports_duration(self):
        reg = _registry_with(
            StateMigration(version=2, migration_fn=_add_field_v2, description="add field_x"),
            StateMigration(version=3, migration_fn=_rename_field_v3, description="rename to field_y"),
        )
        original = {"a": 1}

        result = migrate(original, from_version=1, to_version=3, registry=reg)

        assert result.success is True
        assert result.migrations_applied == [2, 3]
        assert result.state == {"a": 1, "field_y": "v2-default"}
        assert result.error is None
        assert result.duration_ms >= 0.0
        # StateMigration.apply deep-copies: the caller's dict is never mutated.
        assert original == {"a": 1}

    def test_partial_path_from_intermediate_version(self):
        reg = _registry_with(
            StateMigration(version=2, migration_fn=_add_field_v2),
            StateMigration(version=3, migration_fn=_rename_field_v3),
        )

        result = migrate({"a": 1, "field_x": "already-v2"}, from_version=2, to_version=3, registry=reg)

        assert result.success is True
        assert result.migrations_applied == [3]
        assert result.state == {"a": 1, "field_y": "already-v2"}


class TestMigrateFailureMidPath:
    def test_stops_at_failing_migration_and_returns_state_so_far(self):
        def _boom(state: dict) -> dict:
            raise ValueError("cannot rename")

        reg = _registry_with(
            StateMigration(version=2, migration_fn=_add_field_v2),
            StateMigration(version=3, migration_fn=_boom),
        )
        original = {"a": 1}

        result = migrate(original, from_version=1, to_version=3, registry=reg)

        assert result.success is False
        assert result.migrations_applied == [2]
        # State reflects the last successful migration, not the original.
        assert result.state == {"a": 1, "field_x": "v2-default"}
        assert result.error is not None
        assert "version 3" in result.error
        assert "cannot rename" in result.error
        assert result.duration_ms >= 0.0
        # The caller's dict is still untouched.
        assert original == {"a": 1}
