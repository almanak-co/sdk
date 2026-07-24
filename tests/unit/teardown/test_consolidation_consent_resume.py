"""VIB-5938 — retired teardown sweep consent is fail-closed everywhere.

Manual request provenance was previously persisted as consent to bypass the
tracked-inventory swap clamp.  That is not informed authorization to consume
commingled wallet funds.  The legacy field and envelope helper remain only for
source/wire compatibility; every boundary scrubs them to tracked-only.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownState,
    TeardownStatus,
    decode_consolidation_consent,
    encode_consolidation_consent,
)
from almanak.framework.teardown.serialization import teardown_state_from_json, teardown_state_to_json
from almanak.framework.teardown.state_manager import TeardownStateAdapter
from almanak.framework.teardown.teardown_manager import TeardownManager


def _make_state(
    *,
    consolidation_consent: bool = True,
    config_json: str = '{"target_token":"USDC","__consolidation_consent__":true}',
) -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id="td-legacy-consent",
        deployment_id="deployment:abc123",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
        pending_intents_json='[{"intent_type":"SWAP"}]',
        config_json=config_json,
        consolidation_consent=consolidation_consent,
    )


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "not json",
        "[]",
        '{"__consolidation_consent__":true}',
        '{"__consolidation_consent__":"true"}',
        '{"__consolidation_consent__":1}',
    ],
)
def test_legacy_envelopes_never_decode_as_consent(raw: str) -> None:
    assert decode_consolidation_consent(raw) is False


def test_encoder_ignores_true_and_scrubs_legacy_key() -> None:
    encoded = encode_consolidation_consent(
        '{"target_token":"USDC","x":1,"__consolidation_consent__":true}',
        True,
    )
    assert json.loads(encoded) == {"target_token": "USDC", "x": 1}
    assert decode_consolidation_consent(encoded) is False


@pytest.mark.asyncio
async def test_sqlite_round_trip_scrubs_legacy_consent(tmp_path: Path) -> None:
    adapter = TeardownStateAdapter(db_path=tmp_path / "state.db")
    state = _make_state()
    await adapter.save_teardown_state(state)

    loaded = await adapter.get_teardown_state(state.deployment_id)
    assert loaded is not None
    assert loaded.consolidation_consent is False
    assert "__consolidation_consent__" not in json.loads(loaded.config_json)
    assert json.loads(loaded.config_json)["target_token"] == "USDC"


def test_gateway_round_trip_scrubs_legacy_consent() -> None:
    rebuilt = teardown_state_from_json(teardown_state_to_json(_make_state()))
    assert rebuilt.consolidation_consent is False
    assert "__consolidation_consent__" not in json.loads(rebuilt.config_json)


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "deployment:abc123"
    strategy.get_open_positions.return_value = MagicMock()
    strategy.generate_teardown_intents.return_value = []
    return strategy


@pytest.mark.asyncio
async def test_resume_does_not_thread_legacy_consent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = TeardownStateAdapter(db_path=tmp_path / "state.db")
    await adapter.save_teardown_state(_make_state())
    manager = TeardownManager(state_manager=adapter)
    monkeypatch.setattr(
        "almanak.framework.teardown.teardown_manager._warm_oracle_risk_first",
        lambda *args, **kwargs: {},
    )

    captured: dict[str, object] = {}

    async def _fake_execute_intents(*args, **kwargs):
        captured.update(kwargs)
        return MagicMock(success=True)

    manager._execute_intents = AsyncMock(side_effect=_fake_execute_intents)  # type: ignore[method-assign]
    result = await manager.resume("deployment:abc123", _make_strategy(), market=None)

    assert result is not None
    assert result.success is True
    assert "consolidation_consent" not in captured
