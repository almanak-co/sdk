"""Serialization helpers for teardown gateway RPC payloads."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from almanak.framework.teardown.models import TeardownMode, TeardownRequest, TeardownState, TeardownStatus


def teardown_request_to_json(request: TeardownRequest) -> str:
    """Serialize a TeardownRequest for gateway transport."""
    return json.dumps(request.to_dict(), sort_keys=True)


def teardown_request_from_json(raw: str) -> TeardownRequest:
    """Deserialize a TeardownRequest from gateway transport."""
    return TeardownRequest.from_dict(json.loads(raw))


def teardown_state_to_json(state: TeardownState) -> str:
    """Serialize a TeardownState for gateway transport."""
    return json.dumps(_teardown_state_to_dict(state), sort_keys=True)


def teardown_state_from_json(raw: str) -> TeardownState:
    """Deserialize a TeardownState from gateway transport."""
    return _teardown_state_from_dict(json.loads(raw))


def _iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _teardown_state_to_dict(state: TeardownState) -> dict[str, Any]:
    return {
        "teardown_id": state.teardown_id,
        "deployment_id": state.deployment_id,
        "mode": state.mode.value,
        "status": state.status.value,
        "total_intents": state.total_intents,
        "completed_intents": state.completed_intents,
        "current_intent_index": state.current_intent_index,
        "started_at": state.started_at.isoformat(),
        "updated_at": state.updated_at.isoformat(),
        "completed_at": _iso_or_none(state.completed_at),
        "pending_intents_json": state.pending_intents_json,
        "intent_results": state.intent_results,
        "cancel_window_until": _iso_or_none(state.cancel_window_until),
        "config_json": state.config_json,
    }


def _teardown_state_from_dict(data: dict[str, Any]) -> TeardownState:
    started_at = _parse_datetime(data.get("started_at"))
    updated_at = _parse_datetime(data.get("updated_at"))
    if started_at is None or updated_at is None:
        raise ValueError("teardown state requires started_at and updated_at")

    pending_intents_json = data.get("pending_intents_json")
    if pending_intents_json is None:
        pending_intents_json = ""
    elif not isinstance(pending_intents_json, str):
        raise ValueError("pending_intents_json must be a string")

    config_json = data.get("config_json")
    if config_json is None:
        config_json = ""
    elif not isinstance(config_json, str):
        raise ValueError("config_json must be a string")

    intent_results = data.get("intent_results")
    if intent_results is None:
        intent_results = []
    elif not isinstance(intent_results, list):
        raise ValueError("intent_results must be a list")

    return TeardownState(
        teardown_id=data["teardown_id"],
        deployment_id=data["deployment_id"],
        mode=TeardownMode(data["mode"]),
        status=TeardownStatus(data["status"]),
        total_intents=int(data.get("total_intents", 0)),
        completed_intents=int(data.get("completed_intents", 0)),
        current_intent_index=int(data.get("current_intent_index", 0)),
        started_at=started_at,
        updated_at=updated_at,
        completed_at=_parse_datetime(data.get("completed_at")),
        pending_intents_json=pending_intents_json,
        intent_results=intent_results,
        cancel_window_until=_parse_datetime(data.get("cancel_window_until")),
        config_json=config_json,
    )
