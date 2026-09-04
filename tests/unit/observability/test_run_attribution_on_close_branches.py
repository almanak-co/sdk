"""Branch contracts for close-time PnL attribution orchestration."""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from almanak.framework.observability import pnl_attributor


class _CloseEvent:
    def __init__(self, *, attribution_json: str = "{}") -> None:
        self.id = "close-1"
        self.deployment_id = "deployment-1"
        self.position_id = "position-1"
        self.position_type = "LP"
        self.event_type = "CLOSE"
        self.token0 = "USDC"
        self.token1 = "WETH"
        self.chain = "arbitrum"
        self.timestamp = "2026-01-03T00:00:00+00:00"
        self.value_usd = "0"
        self.gas_usd = "0"
        self.protocol_fees_usd = "0"
        self.attribution_json = attribution_json
        self.attribution_version = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "deployment_id": self.deployment_id,
            "position_id": self.position_id,
            "position_type": self.position_type,
            "event_type": self.event_type,
            "token0": self.token0,
            "token1": self.token1,
            "chain": self.chain,
            "timestamp": self.timestamp,
            "value_usd": self.value_usd,
            "gas_usd": self.gas_usd,
            "protocol_fees_usd": self.protocol_fees_usd,
            "attribution_json": self.attribution_json,
        }


def _history(close_id: str = "close-1") -> list[dict[str, object]]:
    return [
        {
            "id": "open-1",
            "event_type": "OPEN",
            "position_type": "LP",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "value_usd": "0",
            "gas_usd": "0",
            "protocol_fees_usd": "0",
            "attribution_json": "{}",
        },
        {
            "id": "collect-1",
            "event_type": "LP_COLLECT_FEES",
            "timestamp": "2026-01-02T00:00:00+00:00",
            "value_usd": "0",
        },
        {
            "id": close_id,
            "event_type": "CLOSE",
            "timestamp": "2026-01-03T00:00:00+00:00",
        },
    ]


class _UpdateStore:
    def __init__(self, history: list[dict[str, object]] | None = None) -> None:
        self.history = history if history is not None else _history()
        self.updates: list[tuple[object, ...]] = []

    async def get_position_history(self, deployment_id: str, position_id: str) -> list[dict[str, object]]:
        assert (deployment_id, position_id) == ("deployment-1", "position-1")
        return self.history

    async def update_position_attribution(
        self,
        event_id: str,
        attribution: str,
        version: int,
        *,
        deployment_id: str,
    ) -> None:
        self.updates.append((event_id, attribution, version, deployment_id))


class _SaveStore:
    def __init__(self) -> None:
        self.saved: list[object] = []

    async def get_position_history(self, deployment_id: str, position_id: str) -> list[dict[str, object]]:
        return _history()

    async def save_position_event(self, event: object) -> None:
        self.saved.append(event)


class _FailingUpdateStore(_UpdateStore):
    async def update_position_attribution(
        self,
        event_id: str,
        attribution: str,
        version: int,
        *,
        deployment_id: str,
    ) -> None:
        raise RuntimeError("persistence unavailable")


@pytest.mark.asyncio
async def test_preserves_measured_zero_and_scoped_versioned_update(monkeypatch: pytest.MonkeyPatch) -> None:
    prices = {"USDC": "0", "WETH": "2000"}
    compute = Mock(wraps=pnl_attributor.compute_attribution)
    monkeypatch.setattr(pnl_attributor, "_fetch_latest_token_prices", AsyncMock(return_value=prices))
    monkeypatch.setattr(pnl_attributor, "compute_attribution", compute)
    close_event = _CloseEvent(attribution_json=json.dumps({"funding_fee_usd": "0"}))
    store = _UpdateStore()

    attribution = await pnl_attributor.run_attribution_on_close(store, close_event)

    data = json.loads(attribution)
    assert data["version"] == pnl_attributor.CURRENT_VERSION
    assert data["principal_deposited_usd"] == "0"
    assert data["principal_recovered_usd"] == "0"
    assert data["fee_pnl_usd"] == "0"
    assert data["collected_fees_usd"] == "0"
    assert data["current_prices"] == prices
    assert json.loads(compute.call_args.args[1]["attribution_json"]) == {
        "funding_fee_usd": "0",
        "current_prices": prices,
    }
    assert close_event.attribution_version == pnl_attributor.CURRENT_VERSION
    assert store.updates == [
        ("close-1", attribution, pnl_attributor.CURRENT_VERSION, "deployment-1"),
    ]


@pytest.mark.parametrize("raw_sidecar", ["{", "[]"])
@pytest.mark.asyncio
async def test_invalid_close_sidecar_is_replaced_only_with_current_prices(
    monkeypatch: pytest.MonkeyPatch,
    raw_sidecar: str,
) -> None:
    prices = {"USDC": "1"}
    compute = Mock(return_value="{}")
    monkeypatch.setattr(pnl_attributor, "_fetch_latest_token_prices", AsyncMock(return_value=prices))
    monkeypatch.setattr(pnl_attributor, "compute_attribution", compute)
    close_event = _CloseEvent(attribution_json=raw_sidecar)
    store = _UpdateStore()

    attribution = await pnl_attributor.run_attribution_on_close(store, close_event)

    assert attribution == "{}"
    enriched_close = compute.call_args.args[1]
    assert json.loads(enriched_close["attribution_json"]) == {"current_prices": prices}
    assert store.updates == []


@pytest.mark.asyncio
async def test_empty_attribution_is_not_persisted_without_prices(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pnl_attributor, "_fetch_latest_token_prices", AsyncMock(return_value=None))
    monkeypatch.setattr(pnl_attributor, "compute_attribution", Mock(return_value="{}"))
    close_event = _CloseEvent()
    store = _UpdateStore()

    attribution = await pnl_attributor.run_attribution_on_close(store, close_event)

    assert attribution == "{}"
    assert close_event.attribution_json == "{}"
    assert close_event.attribution_version == 0
    assert store.updates == []


@pytest.mark.asyncio
async def test_fallback_store_saves_versioned_event_without_price_sidecar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pnl_attributor, "_fetch_latest_token_prices", AsyncMock(return_value=None))
    close_event = _CloseEvent()
    store = _SaveStore()

    attribution = await pnl_attributor.run_attribution_on_close(store, close_event)

    assert json.loads(attribution)["version"] == pnl_attributor.CURRENT_VERSION
    assert "current_prices" not in json.loads(attribution)
    assert close_event.attribution_json == attribution
    assert close_event.attribution_version == pnl_attributor.CURRENT_VERSION
    assert store.saved == [close_event]


@pytest.mark.parametrize("computed", ["not-json", "[]"])
@pytest.mark.asyncio
async def test_non_object_computed_attribution_is_persisted_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    computed: str,
) -> None:
    monkeypatch.setattr(
        pnl_attributor,
        "_fetch_latest_token_prices",
        AsyncMock(return_value={"USDC": "1"}),
    )
    monkeypatch.setattr(pnl_attributor, "compute_attribution", Mock(return_value=computed))
    close_event = _CloseEvent()
    store = _UpdateStore()

    attribution = await pnl_attributor.run_attribution_on_close(store, close_event)

    assert attribution == computed
    assert store.updates == [
        ("close-1", computed, pnl_attributor.CURRENT_VERSION, "deployment-1"),
    ]


@pytest.mark.asyncio
async def test_persistence_failure_warns_and_returns_computed_attribution(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(pnl_attributor, "_fetch_latest_token_prices", AsyncMock(return_value=None))
    close_event = _CloseEvent()
    store = _FailingUpdateStore()

    with caplog.at_level(logging.WARNING, logger=pnl_attributor.__name__):
        attribution = await pnl_attributor.run_attribution_on_close(store, close_event)

    assert attribution != "{}"
    assert close_event.attribution_json == attribution
    assert close_event.attribution_version == pnl_attributor.CURRENT_VERSION
    assert "Failed to run attribution on close" in caplog.text


@pytest.mark.asyncio
async def test_close_without_to_dict_fails_soft_without_fabricating_zero(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fetch_prices = AsyncMock(return_value={"USDC": "1"})
    monkeypatch.setattr(pnl_attributor, "_fetch_latest_token_prices", fetch_prices)
    close_event = SimpleNamespace(
        id="close-1",
        deployment_id="deployment-1",
        position_id="position-1",
    )
    store = _UpdateStore()

    with caplog.at_level(logging.WARNING, logger=pnl_attributor.__name__):
        attribution = await pnl_attributor.run_attribution_on_close(store, close_event)

    assert attribution == "{}"
    assert store.updates == []
    fetch_prices.assert_not_awaited()
    assert "Failed to run attribution on close" in caplog.text
