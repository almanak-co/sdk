"""VIB-4489: G15 cell unwraps versioned positions_json envelope.

Tests are organized to match the frozen UAT card at
docs/internal/uat-cards/VIB-4489.md (round 5, SHA 6749787a). Card spec
defines the structural accept/reject rule for `positions_json`:

  accept:  list-root OR dict-root with `positions: list` (any other keys ignored)
  reject:  un-parseable JSON, scalar root, dict without `positions`,
           dict where `positions` is not a list
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from almanak.framework.accounting.accountant_test import (
    _cell_g15_multi_period_self_consistency,
)


def _snap(snapshot_id: int, positions_json_value: str | None) -> dict[str, Any]:
    return {"id": snapshot_id, "positions_json": positions_json_value}


def _track_c(snapshot_id: int, n: int = 1) -> list[dict[str, Any]]:
    return [{"snapshot_id": snapshot_id} for _ in range(n)]


# ─── D1.S1 / D2.M3 — lifecycle / envelope-shaped fixture ────────────────


def test_g15_passes_on_envelope_shaped_fixture() -> None:
    snapshots = [
        _snap(
            10,
            json.dumps(
                {
                    "schema_version": 1,
                    "positions": [{"position_type": "SUPPLY", "value_usd": "37.6"}],
                    "metadata": {"source": "test"},
                }
            ),
        )
    ]
    position_state_rows = _track_c(10, n=1)
    result = _cell_g15_multi_period_self_consistency(snapshots, position_state_rows)
    assert result.status == "PASS"
    assert "malformed" not in result.diagnostic
    assert "JSONDecodeError" not in result.diagnostic


# ─── D1.S2 — versioned envelope unwrap (the headline fix) ───────────────


def test_g15_unwraps_versioned_envelope() -> None:
    snapshots = [
        _snap(
            20,
            json.dumps(
                {
                    "schema_version": 1,
                    "positions": [{"position_type": "LP"}, {"position_type": "LP"}],
                }
            ),
        )
    ]
    position_state_rows = _track_c(20, n=2)
    result = _cell_g15_multi_period_self_consistency(snapshots, position_state_rows)
    assert result.status == "PASS"


# ─── D2.M1 — each accepted shape ────────────────────────────────────────


def test_g15_envelope_with_metadata_passes() -> None:
    snapshots = [
        _snap(
            30,
            json.dumps(
                {
                    "schema_version": 1,
                    "positions": [{"position_type": "LP"}],
                    "metadata": {"k": "v"},
                }
            ),
        )
    ]
    result = _cell_g15_multi_period_self_consistency(snapshots, _track_c(30, n=1))
    assert result.status == "PASS"


def test_g15_envelope_without_metadata_passes() -> None:
    snapshots = [
        _snap(
            31,
            json.dumps({"schema_version": 1, "positions": [{"position_type": "LP"}]}),
        )
    ]
    result = _cell_g15_multi_period_self_consistency(snapshots, _track_c(31, n=1))
    assert result.status == "PASS"


def test_g15_minimal_dict_shape_passes() -> None:
    snapshots = [_snap(32, json.dumps({"positions": [{"position_type": "LP"}]}))]
    result = _cell_g15_multi_period_self_consistency(snapshots, _track_c(32, n=1))
    assert result.status == "PASS"


def test_g15_dict_with_unknown_extra_key_passes() -> None:
    snapshots = [
        _snap(
            33,
            json.dumps(
                {"positions": [{"position_type": "LP"}], "unexpected_key": True}
            ),
        )
    ]
    result = _cell_g15_multi_period_self_consistency(snapshots, _track_c(33, n=1))
    assert result.status == "PASS"


def test_g15_legacy_plain_list_shape_passes() -> None:
    snapshots = [_snap(34, json.dumps([{"position_type": "LP"}]))]
    result = _cell_g15_multi_period_self_consistency(snapshots, _track_c(34, n=1))
    assert result.status == "PASS"


def test_g15_envelope_empty_positions_skip_coverage() -> None:
    snapshots = [_snap(35, json.dumps({"schema_version": 1, "positions": []}))]
    track_c = _track_c(35, n=1)
    result = _cell_g15_multi_period_self_consistency(snapshots, track_c)
    assert result.status == "PASS"


def test_g15_legacy_empty_list_skip_coverage() -> None:
    snapshots = [_snap(36, "[]")]
    track_c = _track_c(36, n=1)
    result = _cell_g15_multi_period_self_consistency(snapshots, track_c)
    assert result.status == "PASS"


# ─── D2.M2 — XFAIL preserved when Track-C absent ────────────────────────


def test_g15_xfail_when_no_track_c() -> None:
    snapshots = [
        _snap(
            40,
            json.dumps({"schema_version": 1, "positions": [{"position_type": "LP"}]}),
        )
    ]
    result = _cell_g15_multi_period_self_consistency(snapshots, [])
    assert result.status == "XFAIL"


# ─── D2.M3 — lifecycle: open + cash-only + close, mixed shapes ──────────


def test_g15_mixed_lifecycle_pass() -> None:
    snapshots = [
        _snap(
            50,
            json.dumps({"schema_version": 1, "positions": [{"position_type": "SUPPLY"}]}),
        ),
        _snap(51, json.dumps({"schema_version": 1, "positions": []})),
        _snap(52, json.dumps([{"position_type": "SUPPLY"}])),
    ]
    track_c = _track_c(50, n=1) + _track_c(52, n=1)
    result = _cell_g15_multi_period_self_consistency(snapshots, track_c)
    assert result.status == "PASS"


# ─── D3.F1 — genuinely malformed JSON FAILs ─────────────────────────────


def test_g15_genuinely_malformed_json_fails() -> None:
    snapshots = [_snap(60, "{not valid json")]
    track_c = _track_c(60, n=1)
    result = _cell_g15_multi_period_self_consistency(snapshots, track_c)
    assert result.status == "FAIL"
    assert "malformed positions_json" in result.diagnostic
    assert "60" in result.diagnostic  # snapshot id surfaced


# ─── D3.F2 — every JSON scalar root FAILs ──────────────────────────────


@pytest.mark.parametrize(
    "scalar_value",
    [
        "42",
        "true",
        "false",
        '"foo"',
        "null",
    ],
)
def test_g15_scalar_root_fails(scalar_value: str) -> None:
    snapshots = [_snap(70, scalar_value)]
    track_c = _track_c(70, n=1)
    result = _cell_g15_multi_period_self_consistency(snapshots, track_c)
    assert result.status == "FAIL", f"scalar {scalar_value!r} must be rejected, got {result.status}"
    assert "70" in result.diagnostic
    # Cell must not silently treat any scalar as "no positions".
    assert "Coverage" not in result.diagnostic  # not a PASS coverage message


# ─── D3.F3 — dict missing `positions` key FAILs ────────────────────────


def test_g15_envelope_missing_positions_key_fails() -> None:
    snapshots = [_snap(80, json.dumps({"schema_version": 1, "foo": "bar"}))]
    track_c = _track_c(80, n=1)
    result = _cell_g15_multi_period_self_consistency(snapshots, track_c)
    assert result.status == "FAIL"
    assert "80" in result.diagnostic


# ─── D3.F6 — envelope where `positions` is not a list FAILs ────────────


def test_g15_envelope_positions_not_a_list_fails() -> None:
    snapshots = [_snap(90, json.dumps({"schema_version": 1, "positions": "not-a-list"}))]
    track_c = _track_c(90, n=1)
    result = _cell_g15_multi_period_self_consistency(snapshots, track_c)
    assert result.status == "FAIL"
    assert "90" in result.diagnostic
