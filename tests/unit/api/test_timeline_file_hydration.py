"""Focused tests for the local timeline cache hydration fallback."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.api import timeline


@pytest.fixture(autouse=True)
def _isolated_event_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(timeline, "EVENTS_CACHE_FILE", tmp_path / ".dashboard_events.json")
    timeline._event_store.clear()
    yield
    timeline._event_store.clear()


def _write_cache(data: object) -> None:
    timeline.EVENTS_CACHE_FILE.write_text(json.dumps(data))


def test_missing_cache_file_leaves_memory_unchanged() -> None:
    existing = timeline.TimelineEvent(
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        event_type=timeline.TimelineEventType.CUSTOM,
        description="existing",
        deployment_id="deployment:existing",
    )
    timeline._event_store[existing.deployment_id] = [existing]

    timeline._load_events_from_file()

    assert timeline._event_store == {existing.deployment_id: [existing]}


def test_keyed_cache_hydrates_fields_legacy_types_filtering_and_order() -> None:
    _write_cache(
        {
            "deployment:alpha": [
                {
                    "timestamp": "2026-01-02T03:04:05+00:00",
                    "event_type": "trade",
                    "description": "newer trade",
                    "tx_hash": "0xabc",
                    "deployment_id": "deployment:alpha",
                    "chain": "arbitrum",
                    "details": {},
                    "metadata": {"legacy": True},
                    "cycle_id": "cycle-1",
                    "phase": "EXECUTE",
                    "related_ledger_entry_id": "ledger-1",
                },
                {
                    "timestamp": "2026-01-01T03:04:05+00:00",
                    "event_type": "future_type",
                    "description": "older custom",
                },
                {
                    "timestamp": "2026-01-02T03:04:05+00:00",
                    "event_type": "CUSTOM",
                    "description": "same-time second",
                    "details": {"source": "details"},
                },
            ],
            "deployment:beta": [
                {
                    "timestamp": "2026-01-03T00:00:00+00:00",
                    "event_type": "ERROR",
                    "description": "other deployment",
                }
            ],
        }
    )

    timeline._load_events_from_file()

    alpha = timeline._event_store["deployment:alpha"]
    assert [event.description for event in alpha] == ["newer trade", "same-time second", "older custom"]
    assert alpha[0] == timeline.TimelineEvent(
        timestamp=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        event_type=timeline.TimelineEventType.TRANSACTION_CONFIRMED,
        description="newer trade",
        tx_hash="0xabc",
        deployment_id="deployment:alpha",
        chain="arbitrum",
        details={"legacy": True},
        cycle_id="cycle-1",
        phase="EXECUTE",
        related_ledger_entry_id="ledger-1",
    )
    assert alpha[1].details == {"source": "details"}
    assert alpha[2].event_type is timeline.TimelineEventType.CUSTOM
    assert [
        event.description
        for event in timeline.get_events(
            "deployment:alpha",
            event_type=timeline.TimelineEventType.TRANSACTION_CONFIRMED,
        ).events
    ] == ["newer trade"]
    beta = timeline._event_store["deployment:beta"]
    assert [event.description for event in beta] == ["other deployment"]
    assert beta[0].deployment_id == "deployment:beta"


@pytest.mark.parametrize(
    ("legacy_type", "expected_type"),
    [
        ("DEPOSIT", timeline.TimelineEventType.POSITION_MODIFIED),
        ("WITHDRAW", timeline.TimelineEventType.POSITION_MODIFIED),
        ("REBALANCE", timeline.TimelineEventType.REBALANCE_EXECUTED),
        ("STATE_CHANGE", timeline.TimelineEventType.STRATEGY_STARTED),
    ],
)
def test_legacy_event_types_are_mapped(legacy_type: str, expected_type: timeline.TimelineEventType) -> None:
    _write_cache(
        {
            "deployment:alpha": [
                {
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "event_type": legacy_type,
                }
            ]
        }
    )

    timeline._load_events_from_file()

    assert timeline._event_store["deployment:alpha"][0].event_type is expected_type


def test_flat_legacy_cache_groups_by_exact_identity_and_skips_malformed_rows(caplog) -> None:
    _write_cache(
        [
            {
                "timestamp": "2026-01-01T00:00:00+00:00",
                "description": "alpha",
                "deployment_id": "deployment:alpha",
            },
            None,
            {
                "timestamp": "2026-01-01T00:00:00+00:00",
                "deployment_id": 123,
            },
            {
                "timestamp": "2026-01-02T00:00:00+00:00",
                "description": "beta",
                "deployment_id": "deployment:beta",
            },
            {"timestamp": "2026-01-03T00:00:00+00:00", "description": "unknown"},
        ]
    )

    timeline._load_events_from_file()

    assert set(timeline._event_store) == {"deployment:alpha", "deployment:beta", "unknown"}
    assert timeline._event_store["deployment:alpha"][0].deployment_id == "deployment:alpha"
    assert timeline._event_store["deployment:beta"][0].deployment_id == "deployment:beta"
    assert timeline._event_store["unknown"][0].deployment_id == "unknown"
    assert "Failed to load events from file: cached event is not an object" in caplog.text
    assert "Failed to load events from file: cached deployment_id is not a string" in caplog.text


def test_malformed_keyed_row_does_not_hide_later_valid_row(caplog) -> None:
    _write_cache(
        {
            "deployment:alpha": [
                {"timestamp": "not-a-timestamp", "description": "malformed"},
                None,
                {
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "description": "valid",
                },
            ]
        }
    )

    timeline._load_events_from_file()

    assert [event.description for event in timeline._event_store["deployment:alpha"]] == ["valid"]
    assert caplog.text.count("Failed to load events from file:") == 2


def test_conflicting_embedded_identity_is_rejected_without_hiding_valid_row(caplog) -> None:
    _write_cache(
        {
            "deployment:alpha": [
                {
                    "timestamp": "2026-01-02T00:00:00+00:00",
                    "deployment_id": "deployment:beta",
                    "description": "conflicting",
                },
                {
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "description": "valid",
                },
            ]
        }
    )

    timeline._load_events_from_file()

    assert [event.description for event in timeline._event_store["deployment:alpha"]] == ["valid"]
    assert "cached event deployment_id does not match its bucket" in caplog.text


def test_duplicate_identity_remains_timestamp_and_description() -> None:
    """Preserve the pending ALM-3538 product decision."""
    _write_cache(
        {
            "deployment:alpha": [
                {
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "event_type": "ERROR",
                    "description": "same description",
                },
                {
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "event_type": "CUSTOM",
                    "description": "same description",
                    "related_ledger_entry_id": "distinct-ledger-row",
                },
                {
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "event_type": "CUSTOM",
                    "description": "different description",
                },
                {
                    "timestamp": "2026-01-02T00:00:00+00:00",
                    "event_type": "CUSTOM",
                    "description": "same description",
                },
            ]
        }
    )

    timeline._load_events_from_file()
    timeline._load_events_from_file()

    events = timeline._event_store["deployment:alpha"]
    assert [event.description for event in events] == [
        "same description",
        "same description",
        "different description",
    ]
    assert events[1].event_type is timeline.TimelineEventType.ERROR
    assert events[1].related_ledger_entry_id == ""


@pytest.mark.parametrize("cache_data", [{}, []])
def test_empty_cache_hydrates_nothing(cache_data: object, caplog) -> None:
    _write_cache(cache_data)

    timeline._load_events_from_file()

    assert timeline._event_store == {}
    assert caplog.text == ""


@pytest.mark.parametrize(
    ("cache_data", "expected_keys"),
    [
        (None, {"deployment:existing"}),
        (42, {"deployment:existing"}),
        ("events", {"deployment:existing"}),
        ({"deployment:alpha": None}, {"deployment:existing", "deployment:alpha"}),
    ],
)
def test_malformed_cache_shape_is_fail_soft(cache_data: object, expected_keys: set[str], caplog) -> None:
    existing = timeline.TimelineEvent(
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        event_type=timeline.TimelineEventType.CUSTOM,
        description="existing",
        deployment_id="deployment:existing",
    )
    timeline._event_store[existing.deployment_id] = [existing]
    _write_cache(cache_data)

    timeline._load_events_from_file()

    assert set(timeline._event_store) == expected_keys
    assert timeline._event_store[existing.deployment_id] == [existing]
    assert "Failed to load events from file:" in caplog.text


def test_malformed_json_is_fail_soft(caplog) -> None:
    timeline.EVENTS_CACHE_FILE.write_text("not json")

    timeline._load_events_from_file()

    assert timeline._event_store == {}
    assert "Failed to load events from file:" in caplog.text


def test_cache_read_failure_is_fail_soft(caplog) -> None:
    timeline.EVENTS_CACHE_FILE.write_text("unused")
    existing_store = {"deployment:existing": []}
    timeline._event_store.update(existing_store)

    with patch("builtins.open", side_effect=OSError("read failed")):
        timeline._load_events_from_file()

    assert timeline._event_store == existing_store
    assert "Failed to load events from file: read failed" in caplog.text


def test_cache_exists_failure_is_fail_soft(caplog, monkeypatch: pytest.MonkeyPatch) -> None:
    cache_file = MagicMock(spec=Path)
    cache_file.exists.side_effect = OSError("stat failed")
    monkeypatch.setattr(timeline, "EVENTS_CACHE_FILE", cache_file)

    timeline._load_events_from_file()

    assert timeline._event_store == {}
    assert "Failed to load events from file: stat failed" in caplog.text
