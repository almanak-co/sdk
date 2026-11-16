"""Hyperliquid phase 3 on a HyperEVM mainnet fork: the reference trading lifecycle.

The HyperEVM side is authentic fork evidence: the Safe, Roles, the generated custody
grants and every custody call run against mainnet contract code at the reference anchor.
Trading goes through the gateway path itself: admission, signing with a D9 agent key the
gateway holds, the recorded body, the maybe-sent mark,
the post and recovery. Every HyperCore effect, the venue's signature check included,
comes from the synthesized model (blueprint 30, section 9) seeded from recorded mainnet
answers, so nothing here is evidence of HyperCore's behaviour, Fork Qualification or
promotion evidence.
"""

import json
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.connectors.hyperliquid import corewriter as cw
from almanak.connectors.hyperliquid.custody import (
    Custody,
    approve_agent,
    record_action,
    record_deposit,
    require_activated,
    require_standard_mode,
)
from almanak.connectors.hyperliquid.fork_model import REGISTER
from almanak.connectors.hyperliquid.fork_service import ModelService
from almanak.connectors.hyperliquid.trading_runtime import Trader
from almanak.gateway.hypercore_info import SpotClearinghouseState, UserAbstraction, UserRole, typed
from almanak.gateway.safe_fixture import owner_transaction
from almanak.gateway.signers import VerifiedSigner
from tests.fork.test_hypercore_custody import (
    ANCHOR,
    Backend,
    as_safe,
    clock,
    custody_fork,
    observe,
    settle,
    usdc_balance,
    warp,
)
from tests.hypercore_trading_support import (
    LocalSigner,
    ModelVenue,
    open_store,
    reference_lifecycle,
    seed_model,
)

pytestmark = pytest.mark.fork
ROOT = Path(__file__).resolve().parents[2]
__all__ = ["custody_fork"]


class ForkRun:
    """The custody side through the generated grants, and the fork's clock."""

    def __init__(self, fixture, backend, store):
        self.fixture, self.backend, self.store = fixture, backend, store
        self.model, self.node, self.safe = fixture.model, fixture.node, fixture.safe

    def now(self) -> int:
        return clock(self.node)

    def custody(self) -> Custody:
        return self.store.load().custody

    def keep(self, custody: Custody) -> None:
        self.store.update(lambda s: s.model_copy(update={"custody": custody}))

    def act(self, call) -> None:
        receipt, timestamp = as_safe(self.fixture, call)
        custody = record_action(self.custody(), call, receipt, timestamp)
        self.keep(settle(self.fixture, self.backend, custody))
        assert self.custody().requests[-1].status == "resolved"

    def fund_perp(self, report: dict) -> None:
        """Deposit 30 USDC, owner HYPE for the withdrawal fee, standard mode, the first
        D9 agent and 20 USDC to perp."""
        approve, deposit, revoke = cw.deposit_calls(30_000_000)
        as_safe(self.fixture, approve)
        receipt, timestamp = as_safe(self.fixture, deposit)
        as_safe(self.fixture, revoke)
        custody = record_deposit(self.custody(), receipt, timestamp, 30_000_000)
        self.keep(settle(self.fixture, self.backend, custody))
        require_activated(observe(self.backend, UserRole(user=self.safe)), self.safe)
        owner_transaction(
            self.node.rpc, self.fixture.owner, self.safe, cw.HYPE_SYSTEM, "0x", 10**16
        )
        self.fixture.keeper.advance()
        self.act(cw.standard_mode(self.safe))
        require_standard_mode(observe(self.backend, UserAbstraction(user=self.safe)), self.safe)
        custody, call = approve_agent(self.custody(), self.fixture.keys, self.now())
        self.keep(custody)
        self.act(call)
        self.act(cw.class_transfer(20_000_000, True))
        report["funded"] = [r.model_dump(mode="json") for r in self.custody().requests]

    def cross_hour(self) -> None:
        seconds = self.now() // 1000
        self.advance(3600 - seconds % 3600 + 1)

    def advance(self, seconds: int) -> None:
        warp(self.node, seconds)
        self.fixture.keeper.advance()

    def withdraw_all(self, report: dict, perp: Decimal) -> None:
        """Perp back to spot, then all spot USDC out to the Safe on HyperEVM."""
        if perp > 0:
            self.act(cw.class_transfer(int(perp * 10**6), False))
        amount = self.spot_usdc()
        before = usdc_balance(self.node, self.safe)
        request = self.withdraw(amount)
        assert usdc_balance(self.node, self.safe) - before == amount
        report["withdrawn"] = {"usdc": amount, "request": request}

    def spot_usdc(self) -> int:
        spot = typed(observe(self.backend, SpotClearinghouseState(user=self.safe)))
        return next(int(Decimal(b.total) * 10**6) for b in spot.balances if b.coin == "USDC")

    def withdraw(self, amount: int) -> dict:
        """Action 13 resolves by its send entry and the system transfer a block later."""
        receipt, timestamp = as_safe(self.fixture, cw.withdrawal(amount))
        custody = record_action(self.custody(), cw.withdrawal(amount), receipt, timestamp)
        custody = settle(self.fixture, self.backend, settle(self.fixture, self.backend, custody))
        self.keep(custody)
        assert custody.requests[-1].status == "resolved"
        return custody.requests[-1].model_dump(mode="json")


def test_reference_trading_lifecycle_against_the_model(custody_fork, tmp_path):
    fixture = custody_fork
    meta = seed_model(fixture.model)
    with ModelService(fixture.model) as service:
        venue = ModelVenue(service.url, lambda: clock(fixture.node))
        backend = Backend(fixture, venue.info)
        start = clock(fixture.node) - 1000
        store = open_store(
            tmp_path / "hypercore.db", "hypercore-fork", Custody(safe=fixture.safe), meta, start
        )
        signer = VerifiedSigner(LocalSigner(fixture.keys), LocalSigner.address)
        report = reference_lifecycle(ForkRun(fixture, backend, store), Trader(store, venue, signer))
        venue.close()
    report.update(
        register=REGISTER,
        anchor={"block": ANCHOR[0], "hash": ANCHOR[1]},
        seed=fixture.model.doc["seed"]["source"],
        injected=fixture.model.doc["injected"],
        model=fixture.model.doc,
    )
    path = ROOT / "artifacts/hyperliquid-trading-fork/report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=1, default=str) + "\n")
