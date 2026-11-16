"""The phase 3 reference lifecycle, shared by the offline and fork tests (blueprint 30, 10).

The reference strategy deposits USDC into the Safe's HyperCore account, sets cross
leverage, opens one BTC perpetual position with an `Ioc` order, holds it across an hour
boundary, closes it reduce-only and withdraws to the Safe on HyperEVM. Around that path
the driver injects the faults of section 10's evidence row: withheld answers for an
order and for `updateLeverage` while flat, an order lost before it applied, a position
the venue closes, and an open order outside the strategy's attempts at teardown.

`run` supplies the custody side (HyperEVM transactions and their requests) and the
clock: the offline test mines blocks of a fake chain, the fork test sends the operator's
transactions through the generated Roles grants on an Anvil fork. Every HyperCore effect
comes from the synthesized model.
"""

import gzip
import json
from decimal import Decimal
from pathlib import Path

import pytest
from eth_account import Account

from almanak.connectors.hyperliquid import trading as rules
from almanak.connectors.hyperliquid.effects import Book, liquidations
from almanak.connectors.hyperliquid.trading import Trading, TradingPolicy, resolve_target
from almanak.connectors.hyperliquid.trading_runtime import Trader, require_complete
from almanak.connectors.hyperliquid.trading_store import HyperCoreState, TradingStore
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_exchange import HyperCoreExchange
from almanak.gateway.hypercore_info import HyperCoreInfo, InfoResult, L2Book

RECORDINGS = Path(__file__).parent / "fixtures" / "hyperliquid-mainnet-info.json.gz"
COIN = "BTC"
SIZE = Decimal("0.002")
LEVERAGE = 10
SLIPPAGE_BPS = 50
# Synthesized: a mark at which the 0.002 BTC position from 20 USDC is under maintenance.
LIQUIDATION_MARK = "69000"


def recorded(label: str) -> dict:
    recordings = json.load(gzip.open(RECORDINGS))["recordings"]
    return next(r for r in recordings if r["label"] == label)


def seed_model(model) -> InfoResult:
    """Seed the model from the 4 November 2026 mainnet recordings, retained as the seed's
    own source; returns the `metaAndAssetCtxs` answer markets resolve from."""
    contexts, book = recorded("metaAndAssetCtxs"), recorded("l2Book.BTC")
    source = {
        "api": "https://api.hyperliquid.xyz/info",
        "metaAndAssetCtxs": contexts["received_at"],
        "l2Book.BTC": book["received_at"],
    }
    model.seed(contexts["result"], {COIN: book["result"]}, source)
    return InfoResult.model_validate({k: contexts[k] for k in ("request", "received_at", "result")})


class ModelVenue:
    """The gateway's HyperCore face over the model's loopback service, on the fork clock."""

    def __init__(self, url: str, clock):
        self.info = HyperCoreInfo(model=url, clock=clock)
        self.exchange = HyperCoreExchange(model=url)
        self.clock = clock

    def fetch(self, request):
        return self.info.fetch(request)

    def post(self, body: bytes):
        return self.exchange.post(body)

    def now(self) -> int:
        return self.clock()

    def close(self) -> None:
        self.info.close()
        self.exchange.close()


def open_store(path: Path, deployment: str, custody, meta: InfoResult, start: int) -> TradingStore:
    target = resolve_target(meta, COIN, LEVERAGE, SLIPPAGE_BPS)
    # The model's register states A4, so an order with no record and no fill past its
    # expiry is non-execution here; on mainnet T4 is open and the policy default keeps it.
    policy = TradingPolicy(release_unknown_after_expiry=True)
    trading = Trading(deployment=deployment, safe=custody.safe, targets=(target,), policy=policy)
    store = TradingStore(path)
    store.create(
        HyperCoreState(custody=custody, trading=trading, book=Book(safe=custody.safe, cursor=start))
    )
    return store


def step(report: dict, name: str, **facts) -> None:
    report.setdefault("steps", []).append({"step": name, **facts})


class LocalSigner:
    """The deployment's signer on forks: the gateway holds the D9 agent keys itself,
    behind the same seam `VerifiedSigner` checks."""

    address = Account.from_key("0x" + "11" * 32).address.lower()

    def __init__(self, keys):
        self.keys = keys

    def __getattr__(self, name):
        return getattr(self.keys, name)


def leverage_withheld(run, trader: Trader, report: dict) -> None:
    """Leverage while flat, its answer withheld: resolved from activeAssetData."""
    run.model.withhold("updateLeverage")
    leverage = trader.leverage(COIN)
    assert leverage.status == "sent" and leverage.answer is None
    trader.require_resolved()
    leverage = trader.state().trading.attempt(leverage.id)
    assert leverage.status == "applied" and leverage.evidence
    step(report, "leverage_withheld", attempt=leverage.id, evidence=leverage.evidence[0].result)


def entry_withheld(run, trader: Trader, report: dict) -> None:
    """The entry, its answer withheld: resolved by orderStatus of its own cloid."""
    run.model.withhold("order")
    entry = trader.enter(COIN, True, SIZE)
    assert entry.status == "sent"
    trader.require_resolved()
    entry = trader.state().trading.attempt(entry.id)
    assert entry.status == "applied" and entry.oids
    assert trader.reconcile().book.positions == {COIN: SIZE}
    step(report, "entry_withheld", attempt=entry.id, cloid=entry.cloid, oids=entry.oids)


def hold_across_hour(run, trader: Trader, report: dict) -> None:
    """Funding at the hour boundary is an external effect, applied once."""
    run.cross_hour()
    trader.reconcile()
    funding = [e for e in trader.reconcile().book.effects if e.kind == "funding"]
    assert len(funding) == 1 and funding[0].cash < 0
    step(report, "hold_across_hour", funding=funding[0].entry)


def restart_before_post(run, trader: Trader, report: dict) -> None:
    """The process dies after marking an entry maybe sent and before posting it. A new
    trader over the same durable document cannot resolve it until its expiry passes,
    then releases it, and applies no effect a second time."""
    book = trader.observe(L2Book(coin=COIN))
    attempt = trader.retain(
        lambda s: rules.plan_entry(s.trading, COIN, trader.agent(s), True, SIZE, book, run.now())
    )
    trader.prepare(attempt.id)
    trader.trading(lambda t: rules.sending(t, attempt.id))
    applied = trader.state().book.effects
    fresh = Trader(trader.store, trader.venue, trader.signer)
    with pytest.raises(DataError, match="no evidence"):
        fresh.restart()
    run.advance(trader.state().trading.policy.expiry // 1000 + 2)
    state = fresh.restart()
    assert state.trading.attempt(attempt.id).status == "not_applied"
    assert state.book.effects[: len(applied)] == applied and state.book.positions == {COIN: SIZE}
    step(report, "restart_before_post", attempt=attempt.id)


def reduce_only_exit(run, trader: Trader, report: dict) -> None:
    (exit_order,) = trader.close_positions()
    assert exit_order.status == "applied" and exit_order.request.action.order.reduce_only
    assert trader.reconcile().book.positions == {}
    step(report, "exit", attempt=exit_order.id, oids=exit_order.oids)


def lost_and_replanned(run, trader: Trader, report: dict) -> None:
    """An entry lost before it applied stays unresolved until its expiry, is then
    non-execution, and is planned afresh under a new cloid."""
    run.model.lose("order")
    lost = trader.enter(COIN, True, SIZE)
    assert trader.recover() == (trader.state().trading.attempt(lost.id),)
    run.advance(trader.state().trading.policy.expiry // 1000 + 2)
    assert trader.recover() == ()
    lost = trader.state().trading.attempt(lost.id)
    replanned = trader.enter(COIN, True, SIZE)
    assert lost.status == "not_applied" and replanned.status == "applied"
    assert replanned.cloid != lost.cloid
    trader.reconcile()
    step(report, "lost_and_replanned", lost=lost.id, replanned=replanned.id)


def venue_closed(run, trader: Trader, report: dict) -> None:
    """The venue liquidates the position: its fills and ledger entry are reconciled as
    external effects, and the position is gone without an order of the strategy's."""
    run.model.move_mark(COIN, LIQUIDATION_MARK, "synthesized drop below maintenance margin")
    state = trader.reconcile()
    assert state.book.positions == {} and liquidations(state.book)
    step(report, "venue_closed", effects=[e.key for e in liquidations(state.book)])


def teardown_outside_order(run, trader: Trader, report: dict) -> None:
    """An open order the strategy never placed is reported, never cancelled, and keeps
    teardown incomplete; the USDC still goes back to the Safe on HyperEVM."""
    safe = trader.state().trading.safe
    outside = run.model.place_outside_order(safe, COIN, True, "50000", "0.001")
    assert trader.close_positions() == ()
    verdict = trader.verdict()
    assert not verdict.complete and [o.oid for o in verdict.outside] == [outside]
    with pytest.raises(DataError) as halted:
        require_complete(verdict)
    step(report, "teardown_outside_order", halt=halted.value.kind, verdict=halted.value.details)
    run.withdraw_all(report, trader.state().book.cash)


STEPS = (
    leverage_withheld,
    entry_withheld,
    hold_across_hour,
    restart_before_post,
    reduce_only_exit,
    lost_and_replanned,
    venue_closed,
    teardown_outside_order,
)


def reference_lifecycle(run, trader: Trader) -> dict:
    """Run the phase 3 reference strategy with its faults; returns the run's report."""
    report: dict = {}
    run.fund_perp(report)
    for each in STEPS:
        each(run, trader, report)
    state = trader.state()
    report["book"] = state.book.model_dump(mode="json")
    report["attempts"] = [a.model_dump(mode="json") for a in state.trading.attempts]
    return report
