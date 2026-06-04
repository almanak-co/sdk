"""Regression coverage for runner-hook enrichment before ledger serialization."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import almanak.framework.observability.ledger as ledger_mod
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner


class _Strategy:
    deployment_id = "s1"
    chain = "arbitrum"
    wallet_address = "0x" + "0" * 40


class _Runner(StrategyRunner):
    """Drive the real ledger write while stubbing unrelated leaf dependencies."""

    def __init__(self, *, state_manager: Any, gateway_client: Any) -> None:
        self.state_manager = state_manager
        self.config = RunnerConfig(dry_run=False)
        self._iteration_had_trade = False
        self._gateway_client = gateway_client

    def _get_gateway_client(self) -> Any | None:
        return self._gateway_client

    async def _maybe_save_ledger_with_registry(self, **_kwargs: Any) -> bool:
        return False


class _Registry:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls: list[tuple[Any, Any, str]] = []

    def enrich_result(self, result: Any, *, gateway_client: Any, chain: str) -> None:
        self.events.append("enrich")
        self.calls.append((result, gateway_client, chain))
        result.extracted_data = {"current_tick": 123}


def _swap_intent() -> SwapIntent:
    return SwapIntent(from_token="USDC", to_token="ETH", amount_usd=Decimal("100"))


@pytest.mark.asyncio
async def test_write_ledger_entry_enriches_result_before_ledger_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Connector-owned hooks must populate extracted_data before ledger serialization."""
    from almanak.connectors import _strategy_runner_hook_registry as runner_hook_registry

    events: list[str] = []
    registry = _Registry(events)
    gateway_client = object()
    state_mgr = MagicMock()
    state_mgr.save_ledger_entry = AsyncMock(return_value=None)
    runner = _Runner(state_manager=state_mgr, gateway_client=gateway_client)
    result = SimpleNamespace(extracted_data={})

    def _build_ledger_entry(**kwargs: Any) -> LedgerEntry:
        events.append("build")
        assert kwargs["result"].extracted_data == {"current_tick": 123}
        return LedgerEntry(id="led-1")

    monkeypatch.setattr(runner_hook_registry, "STRATEGY_RUNNER_HOOK_REGISTRY", registry)
    monkeypatch.setattr(ledger_mod, "build_ledger_entry", _build_ledger_entry)

    ledger_id = await runner._write_ledger_entry(
        strategy=_Strategy(),
        intent=_swap_intent(),
        result=result,
        success=True,
        emit_position_event=False,
    )

    assert ledger_id == "led-1"
    assert events == ["enrich", "build"]
    assert registry.calls == [(result, gateway_client, "arbitrum")]
    state_mgr.save_ledger_entry.assert_awaited_once()
