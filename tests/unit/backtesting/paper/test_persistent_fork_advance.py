"""Persistent-fork advancement tests for PaperTrader."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest

from almanak.connectors._strategy_base.yield_poke_base import PokeResult
from almanak.framework.anvil.accounts import ANVIL_DEFAULT_ADDRESS
from almanak.framework.backtesting.paper.config import ForkLifecycle, PaperTraderConfig
from almanak.framework.backtesting.paper.engine import PaperTrader


@dataclass
class _ForkManager:
    rpc_url: str = "http://127.0.0.1:8546"
    current_block: int = 12345
    advance_success: bool = True
    advanced_by: list[int] = field(default_factory=list)

    async def advance_time(self, seconds: int) -> bool:
        self.advanced_by.append(seconds)
        return self.advance_success

    def get_rpc_url(self) -> str:
        return self.rpc_url


@dataclass
class _YieldPoker:
    results: list[PokeResult] = field(default_factory=list)
    error: Exception | None = None
    calls: list[tuple[str, str, str]] = field(default_factory=list)

    async def poke_all(self, chain: str, rpc_url: str, wallet: str) -> list[PokeResult]:
        self.calls.append((chain, rpc_url, wallet))
        if self.error is not None:
            raise self.error
        return self.results


@dataclass
class _PortfolioTracker:
    initial_balances: dict[str, Decimal] = field(default_factory=dict)
    current_balances: dict[str, Decimal] = field(default_factory=dict)

    def start_session(self, **kwargs: Any) -> None:
        pass

    def record_trade(self, trade: Any) -> None:
        pass


def _make_config(**overrides: Any) -> PaperTraderConfig:
    kwargs: dict[str, Any] = {
        "chain": "arbitrum",
        "rpc_url": "https://arb.example/rpc",
        "deployment_id": "persistent-fork-test",
        "initial_eth": Decimal("1"),
        "tick_interval_seconds": 90,
        "fork_lifecycle": ForkLifecycle.PERSISTENT,
        "yield_poker_enabled": True,
        "price_source": "coingecko",
        "strict_price_mode": False,
    }
    kwargs.update(overrides)
    return PaperTraderConfig(**kwargs)


def _make_trader(
    *,
    config: PaperTraderConfig | None = None,
    fork_manager: _ForkManager | None = None,
    yield_poker: _YieldPoker | None = None,
) -> PaperTrader:
    with patch(
        "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
    ), patch(
        "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
    ), patch(
        "almanak.framework.backtesting.paper.engine.DEXTWAPDataProvider"
    ):
        trader = PaperTrader(
            fork_manager=fork_manager or _ForkManager(),  # type: ignore[arg-type]
            portfolio_tracker=_PortfolioTracker(),  # type: ignore[arg-type]
            config=config or _make_config(),
        )
    trader._backtest_id = "persistent-fork-test"
    trader._yield_poker = yield_poker
    return trader


@pytest.mark.asyncio
async def test_advance_persistent_fork_skips_pokes_when_time_advance_fails() -> None:
    fork_manager = _ForkManager(advance_success=False)
    yield_poker = _YieldPoker(results=[PokeResult(protocol="compound_v3", success=True)])
    trader = _make_trader(fork_manager=fork_manager, yield_poker=yield_poker)

    await trader._advance_persistent_fork()

    assert fork_manager.advanced_by == [90]
    assert yield_poker.calls == []


@pytest.mark.asyncio
async def test_advance_persistent_fork_uses_config_rpc_when_fork_url_empty() -> None:
    fork_manager = _ForkManager(rpc_url="")
    yield_poker = _YieldPoker(results=[PokeResult(protocol="compound_v3", success=True)])
    trader = _make_trader(
        config=_make_config(anvil_port=8654),
        fork_manager=fork_manager,
        yield_poker=yield_poker,
    )

    await trader._advance_persistent_fork()

    assert fork_manager.advanced_by == [90]
    assert yield_poker.calls == [("arbitrum", "http://localhost:8654", ANVIL_DEFAULT_ADDRESS)]


@pytest.mark.asyncio
async def test_advance_persistent_fork_swallows_yield_poker_errors() -> None:
    fork_manager = _ForkManager()
    yield_poker = _YieldPoker(error=RuntimeError("poke failed"))
    trader = _make_trader(fork_manager=fork_manager, yield_poker=yield_poker)

    await trader._advance_persistent_fork()

    assert fork_manager.advanced_by == [90]
    assert yield_poker.calls == [("arbitrum", "http://127.0.0.1:8546", ANVIL_DEFAULT_ADDRESS)]
