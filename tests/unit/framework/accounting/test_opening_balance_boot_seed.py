"""Boot-seed integration: opening wallet inventory → FIFO lots (VIB-4394).

Drives the REAL boot path end-to-end:
  real SQLiteStore (persisted first snapshot with wallet_balances)
  → real StateManager.get_first_snapshot_sync
  → real _seed_opening_balance_lots
  → real FIFOBasisStore.seed_wallet_inventory

so the first disposal of pre-existing inventory realizes against a seeded basis.
No SimpleNamespace stands in for the basis store or the state read — only a thin
runner stand-in carries the wired-up real objects (mirroring the attributes
``reconstruct_lending_basis_store`` reads off the runner).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
import pytest_asyncio

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.portfolio import PortfolioSnapshot, TokenBalance, ValueConfidence
from almanak.framework.runner._run_loop_helpers import _seed_opening_balance_lots
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import (
    StateManager,
    StateManagerConfig,
    WarmBackendType,
)

_DEP = "deployment:bootseed4394"
_CHAIN = "arbitrum"
_WALLET = "0x000000000000000000000000000000000000aAaA"
_NOW = datetime(2026, 5, 9, 0, 0, 0, tzinfo=UTC)


class _RunnerStub:
    """Carries exactly the attributes ``_seed_opening_balance_lots`` reads.

    The basis store and state manager are the REAL objects — only the runner
    shell is a stand-in (the runner itself owns hundreds of unrelated fields).
    """

    def __init__(self, state_manager: StateManager, wallet_address: str) -> None:
        self.state_manager = state_manager
        self._lending_basis_store = FIFOBasisStore()
        self._runtime_config = type("RC", (), {"wallet_address": wallet_address})()


class _StrategyStub:
    deployment_id = _DEP
    wallet_address = ""


@pytest_asyncio.fixture
async def state_manager():
    store = SQLiteStore(SQLiteConfig(db_path=":memory:"))
    await store.initialize()
    sm = StateManager(
        StateManagerConfig(warm_backend=WarmBackendType.SQLITE),
        warm_backend=store,
    )
    await sm.initialize()
    yield sm
    await store.close()


async def _persist_snapshot(sm: StateManager, balances: list[TokenBalance]) -> None:
    snapshot = PortfolioSnapshot(
        timestamp=_NOW,
        deployment_id=_DEP,
        total_value_usd=sum((b.value_usd for b in balances), Decimal("0")),
        available_cash_usd=sum((b.value_usd for b in balances), Decimal("0")),
        value_confidence=ValueConfidence.HIGH,
        positions=[],
        wallet_balances=balances,
        chain=_CHAIN,
        iteration_number=0,
    )
    await sm._warm.save_portfolio_snapshot(snapshot)  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_boot_seed_from_persisted_snapshot_enables_first_disposal(state_manager):
    """Opening WETH inventory seeds a lot; the first SWAP disposal realizes against it."""
    sm = state_manager
    await _persist_snapshot(
        sm,
        [
            TokenBalance(
                symbol="WETH",
                balance=Decimal("2"),
                value_usd=Decimal("6000"),
                price_usd=Decimal("3000"),
            ),
        ],
    )

    runner = _RunnerStub(sm, _WALLET)
    seeded = _seed_opening_balance_lots(runner, _StrategyStub(), _DEP)
    assert seeded == 1

    # The seeded lot lives under the swap:<chain>:<wallet> key with the
    # first-snapshot basis, and a disposal realizes against it.
    key = f"swap:{_CHAIN}:{_WALLET.lower()}"
    cost_consumed, unmatched = runner._lending_basis_store.match_swap_disposal(
        deployment_id=_DEP, position_key=key, token="WETH", amount=Decimal("2")
    )
    assert cost_consumed == Decimal("6000")
    assert unmatched == Decimal("0")


@pytest.mark.asyncio
async def test_boot_seed_unmeasured_price_yields_basis_none(state_manager):
    """Empty≠Zero: a snapshot balance with no price_usd seeds a basis-None lot."""
    sm = state_manager
    await _persist_snapshot(
        sm,
        [
            TokenBalance(
                symbol="WETH",
                balance=Decimal("1"),
                value_usd=Decimal("0"),  # value unknown, but balance is measured
                price_usd=None,  # unmeasured price
            ),
        ],
    )

    runner = _RunnerStub(sm, _WALLET)
    seeded = _seed_opening_balance_lots(runner, _StrategyStub(), _DEP)
    assert seeded == 1

    key = f"swap:{_CHAIN}:{_WALLET.lower()}"
    cost_consumed, unmatched = runner._lending_basis_store.match_swap_disposal(
        deployment_id=_DEP, position_key=key, token="WETH", amount=Decimal("1")
    )
    # None (unmeasured), NOT Decimal("0") — no fabricated 100%-gain.
    assert cost_consumed is None
    assert unmatched == Decimal("0")


@pytest.mark.asyncio
async def test_boot_seed_noops_without_first_snapshot(state_manager):
    """No persisted snapshot → seed no-ops (returns 0), no lots minted."""
    runner = _RunnerStub(state_manager, _WALLET)
    seeded = _seed_opening_balance_lots(runner, _StrategyStub(), _DEP)
    assert seeded == 0
    assert list(runner._lending_basis_store.iter_open_wallet_basis_lots()) == []
