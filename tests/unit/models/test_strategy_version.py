"""Tests for strategy version tracking models and VersionManager.

Focus: full branch coverage of ``VersionManager.compare_versions`` (every
found/missing combination, code/config/connector diff branches, and the
metrics-comparison short-circuits), plus the surrounding deploy / rollback /
list / persistence surfaces that share the same fixtures.
"""

import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.models.strategy_version import (
    DeploymentResult,
    PerformanceMetrics,
    StrategyVersion,
    VersionManager,
)


def make_version(
    version_id: str,
    *,
    deployment_id: str = "dep-1",
    code_hash: str = "hash-a",
    code_version: str = "1.0.0",
    config_snapshot: dict[str, Any] | None = None,
    connector_versions: dict[str, str] | None = None,
    performance_metrics: PerformanceMetrics | None = None,
    is_active: bool = False,
) -> StrategyVersion:
    """Build a StrategyVersion with sensible test defaults."""
    return StrategyVersion(
        version_id=version_id,
        deployment_id=deployment_id,
        code_hash=code_hash,
        code_version=code_version,
        config_snapshot=config_snapshot if config_snapshot is not None else {},
        connector_versions=connector_versions if connector_versions is not None else {},
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        performance_metrics=performance_metrics,
        is_active=is_active,
    )


def manager_with(*versions: StrategyVersion, deployment_id: str = "dep-1") -> VersionManager:
    """Seed a VersionManager with pre-built versions carrying distinct ids."""
    manager = VersionManager(deployment_id=deployment_id)
    for version in versions:
        manager._versions[version.version_id] = version
        manager._version_order.append(version.version_id)
    return manager


class TestCompareVersions:
    """Branch coverage for VersionManager.compare_versions."""

    def test_returns_none_when_version_a_missing(self):
        manager = manager_with(make_version("v_b"))
        assert manager.compare_versions("v_missing", "v_b") is None

    def test_returns_none_when_version_b_missing(self):
        manager = manager_with(make_version("v_a"))
        assert manager.compare_versions("v_a", "v_missing") is None

    def test_returns_none_when_both_missing(self):
        manager = VersionManager(deployment_id="dep-1")
        assert manager.compare_versions("v_x", "v_y") is None

    def test_identical_versions_report_no_changes(self):
        shared_config = {"slippage": "0.005"}
        shared_connectors = {"uniswap_v3": "2.0.0"}
        a = make_version("v_a", config_snapshot=dict(shared_config), connector_versions=dict(shared_connectors))
        b = make_version("v_b", config_snapshot=dict(shared_config), connector_versions=dict(shared_connectors))
        manager = manager_with(a, b)

        result = manager.compare_versions("v_a", "v_b")

        assert result == {
            "version_a": "v_a",
            "version_b": "v_b",
            "code_changed": False,
            "code_version_changed": False,
            "config_changes": {"added": [], "removed": [], "changed": {}},
            "connector_changes": {},
            "metrics_comparison": None,
        }

    def test_full_diff_covers_all_change_categories(self):
        a = make_version(
            "v_a",
            code_hash="hash-a",
            code_version="1.0.0",
            config_snapshot={"kept": 1, "changed": "old", "removed": True},
            connector_versions={"same": "1.0", "bumped": "1.0", "dropped": "3.0"},
        )
        b = make_version(
            "v_b",
            code_hash="hash-b",
            code_version="2.0.0",
            config_snapshot={"kept": 1, "changed": "new", "added": False},
            connector_versions={"same": "1.0", "bumped": "2.0", "introduced": "0.1"},
        )
        manager = manager_with(a, b)

        result = manager.compare_versions("v_a", "v_b")

        assert result is not None
        assert result["code_changed"] is True
        assert result["code_version_changed"] is True
        assert result["config_changes"]["added"] == ["added"]
        assert result["config_changes"]["removed"] == ["removed"]
        assert result["config_changes"]["changed"] == {"changed": {"old": "old", "new": "new"}}
        assert result["connector_changes"] == {
            "bumped": {"old": "1.0", "new": "2.0"},
            "dropped": {"old": "3.0", "new": None},
            "introduced": {"old": None, "new": "0.1"},
        }
        assert result["metrics_comparison"] is None

    def test_metrics_comparison_with_both_sharpe_ratios_set(self):
        metrics_a = PerformanceMetrics(
            net_pnl_usd=Decimal("100"),
            sharpe_ratio=Decimal("1.5"),
            max_drawdown=Decimal("0.10"),
        )
        metrics_b = PerformanceMetrics(
            net_pnl_usd=Decimal("250"),
            sharpe_ratio=Decimal("2.0"),
            max_drawdown=Decimal("0.05"),
        )
        manager = manager_with(
            make_version("v_a", performance_metrics=metrics_a),
            make_version("v_b", performance_metrics=metrics_b),
        )

        result = manager.compare_versions("v_a", "v_b")

        assert result is not None
        assert result["metrics_comparison"] == {
            "net_pnl_diff_usd": "150",
            "sharpe_diff": "0.5",
            "max_drawdown_diff": "-0.05",
        }

    def test_metrics_comparison_treats_missing_sharpe_as_zero(self):
        manager = manager_with(
            make_version("v_a", performance_metrics=PerformanceMetrics(sharpe_ratio=None)),
            make_version("v_b", performance_metrics=PerformanceMetrics(sharpe_ratio=Decimal("1.25"))),
        )

        result = manager.compare_versions("v_a", "v_b")

        assert result is not None
        assert result["metrics_comparison"]["sharpe_diff"] == "1.25"

    @pytest.mark.parametrize(
        ("metrics_a", "metrics_b"),
        [
            (PerformanceMetrics(), None),
            (None, PerformanceMetrics()),
            (None, None),
        ],
        ids=["only_a_has_metrics", "only_b_has_metrics", "neither_has_metrics"],
    )
    def test_metrics_comparison_none_unless_both_sides_present(self, metrics_a, metrics_b):
        manager = manager_with(
            make_version("v_a", performance_metrics=metrics_a),
            make_version("v_b", performance_metrics=metrics_b),
        )

        result = manager.compare_versions("v_a", "v_b")

        assert result is not None
        assert result["metrics_comparison"] is None


class TestVersionManagerStatics:
    def test_compute_code_hash_is_sha256_of_utf8(self):
        code = "def strategy(): pass"
        assert VersionManager.compute_code_hash(code) == hashlib.sha256(code.encode("utf-8")).hexdigest()

    def test_generate_version_id_with_explicit_timestamp(self):
        ts = datetime(2026, 7, 24, 12, 30, 45, tzinfo=UTC)
        assert VersionManager.generate_version_id("dep-1", ts) == "v_dep-1_20260724123045"

    def test_generate_version_id_defaults_to_now(self):
        version_id = VersionManager.generate_version_id("dep-1")
        assert version_id.startswith("v_dep-1_")
        assert len(version_id) == len("v_dep-1_") + 14


class TestDeployVersion:
    def test_rejects_empty_code_hash(self):
        manager = VersionManager(deployment_id="dep-1")
        result = manager.deploy_version(code_hash="", code_version="1.0.0", config_snapshot={})
        assert result.success is False
        assert result.error == "code_hash is required"
        assert result.version is None

    def test_rejects_empty_code_version(self):
        manager = VersionManager(deployment_id="dep-1")
        result = manager.deploy_version(code_hash="abc", code_version="", config_snapshot={})
        assert result.success is False
        assert result.error == "code_version is required"

    def test_first_deploy_activates_version_and_fires_callback(self):
        deployed: list[StrategyVersion] = []
        manager = VersionManager(deployment_id="dep-1", on_deploy=deployed.append)

        result = manager.deploy_version(
            code_hash="abc123",
            code_version="1.0.0",
            config_snapshot={"slippage": "0.005"},
            connector_versions={"uniswap_v3": "2.0.0"},
            created_by="operator@example.com",
            notes="initial",
        )

        assert result.success is True
        assert result.previous_version_id is None
        assert result.version is not None
        assert result.version.is_active is True
        assert result.version.created_by == "operator@example.com"
        assert result.version.notes == "initial"
        assert manager.get_active_version() is result.version
        assert manager.get_version_count() == 1
        assert deployed == [result.version]

    def test_second_deploy_deactivates_previous_version(self):
        manager = VersionManager(deployment_id="dep-1")
        # The real id generator has second-level resolution; two deploys in the
        # same second would collide. Force distinct ids for this test.
        ids = iter(["v_1", "v_2"])
        manager.generate_version_id = lambda deployment_id, timestamp=None: next(ids)

        first = manager.deploy_version(code_hash="h1", code_version="1.0.0", config_snapshot={})
        second = manager.deploy_version(code_hash="h2", code_version="1.1.0", config_snapshot={})

        assert second.success is True
        assert second.previous_version_id == first.version.version_id
        assert first.version.is_active is False
        assert manager.get_active_version() is second.version
        assert manager.get_version_count() == 2

    def test_deploy_callback_failure_does_not_fail_deploy(self):
        def broken_callback(_version: StrategyVersion) -> None:
            raise RuntimeError("callback boom")

        manager = VersionManager(deployment_id="dep-1", on_deploy=broken_callback)
        result = manager.deploy_version(code_hash="abc", code_version="1.0.0", config_snapshot={})
        assert result.success is True

    def test_connector_versions_default_to_empty_dict(self):
        manager = VersionManager(deployment_id="dep-1")
        result = manager.deploy_version(code_hash="abc", code_version="1.0.0", config_snapshot={})
        assert result.version.connector_versions == {}


class TestRollback:
    def test_rollback_to_unknown_version_fails(self):
        manager = manager_with(make_version("v_1", is_active=True))
        result = manager.rollback("v_missing")
        assert result.success is False
        assert result.error == "Version v_missing not found"

    def test_rollback_without_active_version_fails(self):
        manager = manager_with(make_version("v_1", is_active=False))
        result = manager.rollback("v_1")
        assert result.success is False
        assert result.error == "No active version to rollback from"

    def test_rollback_to_currently_active_version_fails(self):
        manager = manager_with(make_version("v_1", is_active=True))
        result = manager.rollback("v_1")
        assert result.success is False
        assert result.error == "Cannot rollback to the currently active version v_1"

    def test_successful_rollback_copies_target_and_deactivates_current(self):
        callback_args: list[tuple[StrategyVersion, StrategyVersion]] = []
        target = make_version(
            "v_old",
            code_hash="hash-old",
            code_version="1.0.0",
            config_snapshot={"slippage": "0.001"},
            connector_versions={"uniswap_v3": "1.0.0"},
        )
        current = make_version("v_new", code_hash="hash-new", code_version="2.0.0", is_active=True)
        manager = manager_with(target, current)
        manager._on_rollback = lambda old, new: callback_args.append((old, new))

        result = manager.rollback("v_old", rolled_back_by="operator@example.com")

        assert result.success is True
        assert result.previous_version_id == "v_new"
        rolled = result.version
        assert rolled is not None
        assert rolled.code_hash == "hash-old"
        assert rolled.code_version == "1.0.0"
        assert rolled.config_snapshot == {"slippage": "0.001"}
        assert rolled.config_snapshot is not target.config_snapshot
        assert rolled.connector_versions == {"uniswap_v3": "1.0.0"}
        assert rolled.connector_versions is not target.connector_versions
        assert rolled.is_active is True
        assert rolled.rollback_from == "v_new"
        assert rolled.created_by == "operator@example.com"
        assert rolled.notes == "Rollback from v_new to v_old"
        assert current.is_active is False
        assert manager.get_active_version() is rolled
        assert callback_args == [(current, rolled)]

    def test_successful_rollback_without_callback_configured(self):
        manager = manager_with(
            make_version("v_old"),
            make_version("v_new", is_active=True),
        )

        result = manager.rollback("v_old")

        assert result.success is True
        assert result.version.rollback_from == "v_new"

    def test_rollback_callback_failure_does_not_fail_rollback(self):
        def broken_callback(_old: StrategyVersion, _new: StrategyVersion) -> None:
            raise RuntimeError("callback boom")

        manager = manager_with(
            make_version("v_old"),
            make_version("v_new", is_active=True),
        )
        manager._on_rollback = broken_callback

        result = manager.rollback("v_old")
        assert result.success is True


class TestListAndQuery:
    def test_get_active_version_none_when_no_versions(self):
        manager = VersionManager(deployment_id="dep-1")
        assert manager.get_active_version() is None

    def test_get_active_version_none_when_all_inactive(self):
        manager = manager_with(make_version("v_1"), make_version("v_2"))
        assert manager.get_active_version() is None

    def test_get_version_missing_returns_none(self):
        manager = VersionManager(deployment_id="dep-1")
        assert manager.get_version("v_missing") is None

    def test_list_versions_newest_first_with_pagination(self):
        manager = manager_with(make_version("v_1"), make_version("v_2"), make_version("v_3"))

        assert [v.version_id for v in manager.list_versions()] == ["v_3", "v_2", "v_1"]
        assert [v.version_id for v in manager.list_versions(limit=1)] == ["v_3"]
        assert [v.version_id for v in manager.list_versions(limit=2, offset=1)] == ["v_2", "v_1"]
        assert manager.list_versions(offset=3) == []

    def test_list_versions_without_metrics_returns_stripped_copies(self):
        metrics = PerformanceMetrics(total_trades=7)
        original = make_version("v_1", performance_metrics=metrics, is_active=True)
        manager = manager_with(original)

        [listed] = manager.list_versions(include_metrics=False)

        assert listed is not original
        assert listed.performance_metrics is None
        assert listed.version_id == original.version_id
        assert listed.is_active is True
        # The stored version keeps its metrics.
        assert original.performance_metrics is metrics

    def test_list_versions_skips_order_entries_missing_from_store(self):
        manager = manager_with(make_version("v_1"))
        manager._version_order.append("v_stale")

        assert [v.version_id for v in manager.list_versions()] == ["v_1"]

    def test_update_metrics_missing_version_returns_false(self):
        manager = VersionManager(deployment_id="dep-1")
        assert manager.update_metrics("v_missing", PerformanceMetrics()) is False

    def test_update_metrics_sets_metrics_on_version(self):
        version = make_version("v_1")
        manager = manager_with(version)
        metrics = PerformanceMetrics(total_pnl_usd=Decimal("42"))

        assert manager.update_metrics("v_1", metrics) is True
        assert version.performance_metrics is metrics

    def test_clear_all_versions_empties_store_and_order(self):
        manager = manager_with(make_version("v_1"), make_version("v_2"))
        manager.clear_all_versions()
        assert manager.get_version_count() == 0
        assert manager.list_versions() == []


class TestManagerPersistence:
    def test_to_dict_from_dict_roundtrip_preserves_versions_and_order(self):
        rollback_calls: list[tuple[StrategyVersion, StrategyVersion]] = []
        manager = manager_with(
            make_version("v_1"),
            make_version("v_2", is_active=True, performance_metrics=PerformanceMetrics(total_trades=3)),
        )

        restored = VersionManager.from_dict(
            manager.to_dict(),
            on_rollback=lambda old, new: rollback_calls.append((old, new)),
        )

        assert restored.deployment_id == "dep-1"
        assert restored.get_version_count() == 2
        assert [v.version_id for v in restored.list_versions()] == ["v_2", "v_1"]
        assert restored.get_active_version().version_id == "v_2"
        assert restored.get_version("v_2").performance_metrics.total_trades == 3

        # Restored manager is fully operational, including the wired callback.
        result = restored.rollback("v_1")
        assert result.success is True
        assert len(rollback_calls) == 1

    def test_from_dict_with_minimal_payload(self):
        restored = VersionManager.from_dict({"deployment_id": "dep-9"})
        assert restored.deployment_id == "dep-9"
        assert restored.get_version_count() == 0
        assert restored.list_versions() == []


class TestPerformanceMetrics:
    def test_to_dict_with_optional_fields_absent(self):
        assert PerformanceMetrics().to_dict() == {
            "total_pnl_usd": "0",
            "net_pnl_usd": "0",
            "sharpe_ratio": None,
            "max_drawdown": "0",
            "win_rate": None,
            "total_trades": 0,
            "total_gas_usd": "0",
            "avg_trade_size_usd": "0",
            "uptime_seconds": 0,
            "measurement_start": None,
            "measurement_end": None,
        }

    def test_roundtrip_with_all_fields_set(self):
        metrics = PerformanceMetrics(
            total_pnl_usd=Decimal("100.5"),
            net_pnl_usd=Decimal("95.25"),
            sharpe_ratio=Decimal("1.8"),
            max_drawdown=Decimal("0.12"),
            win_rate=Decimal("0.6"),
            total_trades=42,
            total_gas_usd=Decimal("5.25"),
            avg_trade_size_usd=Decimal("1000"),
            uptime_seconds=3600,
            measurement_start=datetime(2026, 1, 1, tzinfo=UTC),
            measurement_end=datetime(2026, 1, 2, tzinfo=UTC),
        )
        assert PerformanceMetrics.from_dict(metrics.to_dict()) == metrics

    def test_from_dict_with_empty_payload_uses_defaults(self):
        assert PerformanceMetrics.from_dict({}) == PerformanceMetrics()


class TestStrategyVersionSerialization:
    def test_roundtrip_with_metrics_and_rollback_fields(self):
        version = make_version(
            "v_1",
            performance_metrics=PerformanceMetrics(total_trades=5),
            is_active=True,
        )
        version.rollback_from = "v_0"
        version.notes = "rolled"

        restored = StrategyVersion.from_dict(version.to_dict())

        assert restored == version

    def test_from_dict_minimal_payload_uses_defaults(self):
        restored = StrategyVersion.from_dict(
            {
                "version_id": "v_1",
                "deployment_id": "dep-1",
                "code_hash": "abc",
                "code_version": "1.0.0",
                "created_at": "2026-01-01T00:00:00+00:00",
            }
        )
        assert restored.config_snapshot == {}
        assert restored.connector_versions == {}
        assert restored.created_by == "system"
        assert restored.performance_metrics is None
        assert restored.is_active is False
        assert restored.rollback_from is None
        assert restored.notes is None


class TestDeploymentResultSerialization:
    def test_to_dict_with_version(self):
        version = make_version("v_1")
        result = DeploymentResult(success=True, version=version, previous_version_id="v_0")
        assert result.to_dict() == {
            "success": True,
            "version": version.to_dict(),
            "error": None,
            "previous_version_id": "v_0",
        }

    def test_to_dict_without_version(self):
        result = DeploymentResult(success=False, error="boom")
        assert result.to_dict() == {
            "success": False,
            "version": None,
            "error": "boom",
            "previous_version_id": None,
        }
