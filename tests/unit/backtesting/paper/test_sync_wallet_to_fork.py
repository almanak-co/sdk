"""Characterization tests for ``PaperTrader._sync_wallet_to_fork``."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.paper.config import PaperTraderConfig
from almanak.framework.backtesting.paper.engine import PaperTrader


@dataclass
class _MockForkManager:
    funded_eth: list[tuple[str, Decimal]] = field(default_factory=list)
    funded_tokens: list[tuple[str, dict[str, Decimal]]] = field(default_factory=list)
    fund_wallet_success: bool = True
    fund_tokens_success: bool = True

    async def fund_wallet(self, wallet_address: str, amount: Decimal) -> bool:
        self.funded_eth.append((wallet_address, amount))
        return self.fund_wallet_success

    async def fund_tokens(self, wallet_address: str, balances: dict[str, Decimal]) -> bool:
        self.funded_tokens.append((wallet_address, dict(balances)))
        return self.fund_tokens_success

    def get_rpc_url(self) -> str:
        return "http://127.0.0.1:8546"


@dataclass
class _MockPortfolioTracker:
    initial_balances: dict[str, Decimal] = field(default_factory=dict)
    current_balances: dict[str, Decimal] = field(default_factory=dict)


class _Strategy:
    deployment_id = "sync-wallet-strategy"

    def decide(self, market: Any) -> None:
        return None


def _make_config(**overrides: Any) -> PaperTraderConfig:
    kwargs: dict[str, Any] = {
        "chain": "arbitrum",
        "rpc_url": "https://arb.example/rpc",
        "deployment_id": "sync-wallet-strategy",
        "price_source": "coingecko",
    }
    kwargs.update(overrides)
    return PaperTraderConfig(**kwargs)


def _make_trader(
    *,
    config: PaperTraderConfig | None = None,
    tracker: _MockPortfolioTracker | None = None,
    fork_manager: _MockForkManager | None = None,
) -> tuple[PaperTrader, _MockForkManager, _MockPortfolioTracker]:
    with (
        patch("almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"),
        patch("almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"),
        patch("almanak.framework.backtesting.paper.engine.DEXTWAPDataProvider"),
    ):
        fork = fork_manager or _MockForkManager()
        portfolio = tracker or _MockPortfolioTracker()
        trader = PaperTrader(
            fork_manager=fork,
            portfolio_tracker=portfolio,
            config=config or _make_config(),
        )
    trader._price_aggregator = MagicMock()
    trader._chainlink_provider = None
    trader._twap_provider = None
    trader._rsi_calculator = None
    trader._validate_bootstrap = AsyncMock()
    return trader, fork, portfolio


@pytest.mark.asyncio
async def test_sync_wallet_uses_current_tracker_balances_for_fork_refresh() -> None:
    tracker = _MockPortfolioTracker(
        initial_balances={"ETH": Decimal("10"), "DAI": Decimal("50")},
        current_balances={"ETH": Decimal("2"), "USDC": Decimal("25")},
    )
    trader, fork, _portfolio = _make_trader(tracker=tracker)

    await trader._sync_wallet_to_fork(use_initial=False)

    assert fork.funded_eth == [("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266", Decimal("2"))]
    assert fork.funded_tokens == [
        ("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266", {"USDC": Decimal("25")})
    ]
    trader._validate_bootstrap.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_wallet_initial_bootstrap_funds_config_balances_and_validates_tokens() -> None:
    config = _make_config(
        initial_eth=Decimal("5"),
        initial_tokens={"USDC": Decimal("100")},
    )
    tracker = _MockPortfolioTracker(current_balances={"ETH": Decimal("1"), "WETH": Decimal("2")})
    trader, fork, _portfolio = _make_trader(config=config, tracker=tracker)

    await trader._sync_wallet_to_fork(use_initial=True)

    assert fork.funded_eth == [("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266", Decimal("5"))]
    assert fork.funded_tokens == [
        ("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266", {"USDC": Decimal("100")})
    ]
    trader._validate_bootstrap.assert_awaited_once_with(
        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        {"USDC": Decimal("100")},
    )


@pytest.mark.asyncio
async def test_sync_wallet_infers_bootstrap_tokens_when_config_has_no_explicit_tokens() -> None:
    inferred = {"USDC": Decimal("150")}
    trader, fork, portfolio = _make_trader()
    trader._current_strategy = _Strategy()

    with patch(
        "almanak.framework.backtesting.paper.bootstrap_inference.infer_token_requirements",
        return_value=inferred,
    ) as infer:
        await trader._sync_wallet_to_fork(use_initial=True)

    infer.assert_called_once_with(trader._current_strategy, "arbitrum")
    assert fork.funded_eth == [("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266", Decimal("10"))]
    assert fork.funded_tokens == [
        ("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266", {"USDC": Decimal("150")})
    ]
    assert portfolio.current_balances["USDC"] == Decimal("150")
    assert portfolio.initial_balances["USDC"] == Decimal("150")
    trader._validate_bootstrap.assert_awaited_once_with(
        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        {"USDC": Decimal("150")},
    )


@pytest.mark.asyncio
async def test_sync_wallet_checks_divergence_when_explicit_tokens_are_configured() -> None:
    explicit_inferred = {"USDC": Decimal("150"), "WETH": Decimal("1")}
    config = _make_config(initial_tokens={"USDC": Decimal("100")})
    trader, fork, portfolio = _make_trader(config=config)
    trader._current_strategy = _Strategy()

    with (
        patch(
            "almanak.framework.backtesting.paper.bootstrap_inference.infer_token_requirements",
            return_value=explicit_inferred,
        ) as infer,
        patch("almanak.framework.backtesting.paper.bootstrap_inference.check_divergence") as check_divergence,
    ):
        await trader._sync_wallet_to_fork(use_initial=True)

    infer.assert_called_once_with(trader._current_strategy, "arbitrum")
    check_divergence.assert_called_once_with(
        {"ETH": Decimal("10"), "USDC": Decimal("100")},
        explicit_inferred,
    )
    assert fork.funded_tokens == [
        ("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266", {"USDC": Decimal("100")})
    ]
    assert "WETH" not in portfolio.current_balances
    trader._validate_bootstrap.assert_awaited_once_with(
        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        {"USDC": Decimal("100")},
    )
