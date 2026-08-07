"""Regression: the settlement reconciler's registry payload carries ``is_long`` (VIB-5572).

The commit lane's ``_complete_registry`` had the measured ``is_long`` boolean on
the settlement event and discarded it, persisting only the ``direction`` display
label. Teardown's close builder consumed ``details["is_long"]`` alone, so a
reconciler-registered perp got NO closing intent, every teardown FAILED, and the
failed teardown entry-blocked the strategy (permanent livelock — e2e evidence
``deployment:194e6b4e8771``). These tests pin the payload contract: the measured
boolean is persisted alongside the label, derived from the SAME normalization
(``perp_direction_label``) so the two can never disagree, and an unmeasured side
stays ``None`` (Empty ≠ Zero — never fabricate a direction).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.runner.perp_settlement_commit import _complete_registry

_POSITION_KEY = "0x" + "ab" * 32


def _event(is_long: Any) -> SimpleNamespace:
    return SimpleNamespace(
        position_key=_POSITION_KEY,
        market="0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
        collateral_token="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        is_long=is_long,
        keeper_tx_hash="0x" + "22" * 32,
        block_number=360000000,
    )


async def _captured_payload(monkeypatch, is_long: Any) -> dict:
    captured: dict[str, Any] = {}

    async def _capture(state_manager, *, ledger, registry, mode):
        captured["registry"] = registry
        captured["mode"] = mode

    import almanak.framework.accounting.commit as commit_mod

    monkeypatch.setattr(commit_mod, "save_ledger_and_registry", _capture)

    reason = await _complete_registry(
        SimpleNamespace(state_manager=object()),
        SimpleNamespace(deployment_id="deployment:194e6b4e8771"),
        submission_ledger=SimpleNamespace(tx_hash="0x" + "33" * 32),
        event=_event(is_long),
        chain="arbitrum",
        protocol="gmx_v2",
        is_open=True,
    )
    assert reason is None, f"registry completion degraded: {reason}"
    return captured["registry"].payload


@pytest.mark.parametrize(
    ("is_long", "expected_bool", "expected_label"),
    [
        (True, True, "long"),
        (False, False, "short"),
        # SQLite round-trips the persisted boolean as an integer; a measured 0
        # is a real short, never an absent side (Empty ≠ Zero).
        (0, False, "short"),
        (1, True, "long"),
    ],
)
@pytest.mark.asyncio
async def test_payload_carries_measured_is_long(monkeypatch, is_long, expected_bool, expected_label):
    payload = await _captured_payload(monkeypatch, is_long)
    assert payload["is_long"] is expected_bool
    assert payload["direction"] == expected_label
    assert payload["source"] == "settlement_reconciler"


@pytest.mark.parametrize("unmeasured", [None, ""])
@pytest.mark.asyncio
async def test_unmeasured_side_stays_none_in_both_keys(monkeypatch, unmeasured):
    payload = await _captured_payload(monkeypatch, unmeasured)
    assert payload["is_long"] is None
    assert payload["direction"] is None
