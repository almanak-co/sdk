"""Capital-flow producer at the metrics hook (VIB-5866 leg B, PR-B).

The producer turns raw ERC-20 ``Transfer`` provenance into
``PortfolioMetrics.deposits_usd`` / ``withdrawals_usd``. Everything here is
driven by a fake gateway-backed web3 handle and a fake state backend — no
sockets, no DB.

The highest-value test in this file is
``test_deposit_before_anchor_is_never_booked``: a deposit that lands between
process boot and the deployment's first ledger transaction is already inside
the read side's tx₁ anchor, so booking it again would print a phantom loss.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from almanak.framework.accounting.capital_flows import (
    TRANSFER_SIG,
    ZERO_ADDRESS,
    clear_provenance_caches,
    pad_address_topic,
)
from almanak.framework.portfolio import PortfolioMetrics, PortfolioSnapshot
from almanak.framework.portfolio.models import TokenBalance
from almanak.framework.runner.capital_flow_state import (
    DETAIL_NO_GATEWAY,
    DETAIL_SCAN_DEFERRED,
    MAX_PENDING_UNCLASSIFIED,
    REASON_CHAIN_UNSCANNABLE,
    REASON_PENDING_OVERFLOW,
    REASON_SCAN_GAP,
    REASON_SHARED_WALLET,
    REASON_UNCLASSIFIED_MATERIAL,
    REASON_UNPRICEABLE_FLOW,
    SCHEMA_VERSION,
    STATUS_MEASURED,
    STATUS_PENDING,
    STATUS_UNMEASURED,
    CapitalFlowRecord,
    PendingUnclassified,
    materiality_threshold,
    project_columns,
    recover_record,
)
from almanak.framework.runner.runner_state import (
    CAPITAL_FLOWS_KEY,
    _build_metrics_for_snapshot,
    _populate_capital_flows,
    _write_valuation_into_strategy_state,
)

WALLET = "0x" + "11" * 20
OUTSIDER = "0x" + "22" * 20
CONTRACT = "0x" + "33" * 20
USDC = "0x" + "aa" * 20
MYSTERY = "0x" + "bb" * 20
CHAIN = "arbitrum"
OTHER_CHAIN = "base"
DEPLOYMENT = "deployment:0123456789ab"


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


def _log(
    *,
    token: str = USDC,
    frm: str,
    to: str,
    amount: int,
    block: int,
    index: int = 0,
    tx: str = "0xfeed",
) -> dict[str, Any]:
    return {
        "address": token,
        "topics": [TRANSFER_SIG, pad_address_topic(frm), pad_address_topic(to)],
        "data": hex(amount),
        "transactionHash": tx,
        "blockNumber": block,
        "logIndex": index,
    }


class FakeEth:
    """Minimal ``web3.eth`` surface: get_logs / get_code / get_transaction."""

    def __init__(
        self,
        *,
        head: int,
        logs: list[dict[str, Any]] | None = None,
        codes: dict[str, str] | None = None,
        txs: dict[str, dict[str, Any]] | None = None,
        logs_raise: Exception | None = None,
        head_raises: Exception | None = None,
    ) -> None:
        self._head = head
        self._logs = logs or []
        self._codes = {k.lower(): v for k, v in (codes or {}).items()}
        self._txs = txs or {}
        self._logs_raise = logs_raise
        self._head_raises = head_raises
        self.get_logs_calls: list[dict[str, Any]] = []

    @property
    def block_number(self) -> int:
        if self._head_raises is not None:
            raise self._head_raises
        return self._head

    def get_logs(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.get_logs_calls.append(params)
        if self._logs_raise is not None:
            raise self._logs_raise
        wanted = params.get("topics") or []
        addresses = {a.lower() for a in (params.get("address") or [])}
        out = []
        for log in self._logs:
            if not (params["fromBlock"] <= log["blockNumber"] <= params["toBlock"]):
                continue
            if addresses and log["address"].lower() not in addresses:
                continue
            if any(w is not None and log["topics"][i] != w for i, w in enumerate(wanted)):
                continue
            out.append(log)
        return out

    def get_code(self, address: str) -> str:
        return self._codes.get(address.lower(), "0x")

    def get_transaction(self, tx_hash: str) -> dict[str, Any]:
        return self._txs[tx_hash.lower()]


class FakeWeb3:
    def __init__(self, eth: FakeEth) -> None:
        self.eth = eth


class FakeStateManager:
    """State backend exposing exactly the seams the producer duck-types."""

    def __init__(
        self,
        *,
        ledger: list[Any] | None = None,
        deployment_ids: list[str] | None = None,
        ledger_supported: bool = True,
    ) -> None:
        self.ledger = ledger if ledger is not None else []
        self.deployment_ids = deployment_ids if deployment_ids is not None else [DEPLOYMENT]
        self.snapshot_mirror: dict[str, Any] | None = None
        self.state_mirror: dict[str, Any] = {}
        self.saved_state: Any | None = None
        if not ledger_supported:
            # Shadow the class method with a non-callable so the producer's
            # ``callable(getter)`` duck-type check routes to the fallback
            # (``del`` on a class-defined method would raise AttributeError).
            self.get_ledger_entries = None  # type: ignore[assignment]

    async def get_ledger_entries(self, deployment_id: str, limit: int = 100, **kwargs: Any) -> list[Any]:
        return list(self.ledger[:limit])

    async def get_all_deployment_ids(self) -> list[str]:
        return list(self.deployment_ids)

    async def get_latest_snapshot(self, deployment_id: str) -> Any:
        if self.snapshot_mirror is None:
            return None
        return SimpleNamespace(snapshot_metadata={CAPITAL_FLOWS_KEY: self.snapshot_mirror})

    async def load_state(self, deployment_id: str) -> Any:
        return SimpleNamespace(deployment_id=deployment_id, state=dict(self.state_mirror), version=1)

    async def save_state(self, state: Any, expected_version: int | None = None) -> Any:
        self.saved_state = state
        return state


def _gateway_ledger_row(
    *,
    tx: str = "0xanchor",
    chain: str = CHAIN,
    epoch: int = 1_784_000_000,
    intent_type: str = "SUPPLY",
    with_tx_hash: bool = True,
) -> dict[str, Any]:
    """A row built by the REAL ``_proto_ledger_to_dict`` from a real proto.

    Deliberately not hand-rolled: run 3 was caused by that projection dropping
    a field, and a hand-copied fake shape would have kept passing while
    production broke. Driving the production projection here means a
    regression in it fails this end-to-end producer test too, not only the
    narrow projection pin in tests/gateway/.

    ``with_tx_hash=False`` mutates the row afterwards to reproduce run 3.
    """
    from almanak.framework.state.gateway_state_manager import _proto_ledger_to_dict
    from almanak.gateway.proto import gateway_pb2

    row = _proto_ledger_to_dict(
        gateway_pb2.LedgerEntryInfo(
            id="led-1",
            deployment_id=DEPLOYMENT,
            intent_type=intent_type,
            token_in="USDC",
            amount_in="1.0",
            token_out="aArbUSDCn",
            amount_out="1.0",
            chain=chain,
            timestamp=epoch,
            tx_hash=tx,
            success=True,
        )
    )
    assert isinstance(row["timestamp"], int), "gateway rows carry int epochs, not datetimes"
    if not with_tx_hash:
        row.pop("tx_hash", None)
    return row


class FakeGatewayStateManager:
    """Mirrors ``GatewayStateManager``: no ``get_ledger_entries`` at all.

    The producer must fall back to the sync, tuple-returning
    ``read_ledger_entries_measured`` and cope with dict rows carrying int
    epoch timestamps. Every managed-gateway run takes this path.
    """

    def __init__(self, *, rows: list[dict[str, Any]] | None = None, measured: bool = True) -> None:
        self.rows = rows if rows is not None else []
        self.measured = measured
        self.snapshot_mirror: dict[str, Any] | None = None
        self.state_mirror: dict[str, Any] = {}
        self.saved_state: Any | None = None

    def read_ledger_entries_measured(self, deployment_id: str) -> tuple[list[dict[str, Any]], bool]:
        return list(self.rows), self.measured

    async def get_latest_snapshot(self, deployment_id: str) -> Any:
        if self.snapshot_mirror is None:
            return None
        return SimpleNamespace(snapshot_metadata={CAPITAL_FLOWS_KEY: self.snapshot_mirror})

    async def load_state(self, deployment_id: str) -> Any:
        return SimpleNamespace(deployment_id=deployment_id, state=dict(self.state_mirror), version=1)

    async def save_state(self, state: Any, expected_version: int | None = None) -> Any:
        self.saved_state = state
        return state


class FakeRunner:
    def __init__(
        self,
        state_manager: FakeStateManager,
        *,
        gateway: Any = object(),
        wallet: str | None = WALLET,
        primary: str = CHAIN,
    ) -> None:
        self.state_manager = state_manager
        self.deployment_id = DEPLOYMENT
        self._last_cycle_id = "cycle-1"
        self.config = SimpleNamespace()
        self._gateway = gateway
        self._wallet = wallet
        self._primary_chain_lower = primary

    def _get_gateway_client(self) -> Any:
        return self._gateway

    def _multichain_wallet_for(self, chain: str) -> str | None:
        return self._wallet


def _ledger_row(*, tx: str = "0xanchor", chain: str = CHAIN, seconds: int = 0) -> SimpleNamespace:
    return SimpleNamespace(
        tx_hash=tx,
        chain=chain,
        timestamp=datetime(2026, 7, 19, 12, 0, seconds, tzinfo=UTC),
    )


def _snapshot(
    *,
    total: str = "1000",
    cash: str = "0",
    prices: dict[str, dict] | None = None,
    balances: list[TokenBalance] | None = None,
) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        timestamp=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        deployment_id=DEPLOYMENT,
        total_value_usd=Decimal(total),
        available_cash_usd=Decimal(cash),
        chain=CHAIN,
        wallet_balances=balances if balances is not None else [TokenBalance("USDC", Decimal("1"), Decimal("1"), USDC)],
        token_prices=prices
        if prices is not None
        else {f"{CHAIN}:{USDC}": {"price_usd": "1", "symbol": "USDC", "decimals": 6}},
    )


def _metrics(*, deposits: Decimal | None = Decimal("0"), withdrawals: Decimal | None = Decimal("0")) -> PortfolioMetrics:
    return PortfolioMetrics(
        deployment_id=DEPLOYMENT,
        timestamp=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
        total_value_usd=Decimal("1000"),
        initial_value_usd=Decimal("1000"),
        deposits_usd=deposits,
        withdrawals_usd=withdrawals,
    )


async def _run(
    runner: FakeRunner,
    metrics: PortfolioMetrics,
    snapshot: PortfolioSnapshot,
    handles: dict[str, FakeWeb3],
) -> None:
    """Invoke the producer with ``get_gateway_web3`` bound to fake handles."""

    def _resolve(client: Any, chain: str, *args: Any, **kwargs: Any) -> FakeWeb3:
        return handles[chain]

    with patch("almanak.framework.web3.get_gateway_web3", side_effect=_resolve):
        await _populate_capital_flows(runner, metrics, snapshot, deployment_id=DEPLOYMENT)


def _persist_mirrors(runner: FakeRunner, snapshot: PortfolioSnapshot) -> None:
    """Mimic the two durable writes the runner performs after the hook."""
    record = snapshot.snapshot_metadata.get(CAPITAL_FLOWS_KEY)
    if record is None:
        return
    runner.state_manager.snapshot_mirror = json.loads(json.dumps(record))
    durable = {k: v for k, v in record.items() if k != "status_detail"}
    runner.state_manager.state_mirror[CAPITAL_FLOWS_KEY] = json.dumps(durable, sort_keys=True)


async def _two_cycles(
    runner: FakeRunner,
    eth: FakeEth,
    *,
    metrics: PortfolioMetrics | None = None,
    advance_head: int = 20,
) -> dict[str, Any]:
    """Run the scan cycle twice: cycle 1 defers, cycle 2 judges the deferral.

    Unclassified transfers are never gated on first sighting (the own-tx
    ledger race), so any assertion about the materiality gate needs the
    recheck cycle to have happened.
    """
    first_metrics = _metrics()
    snapshot = _snapshot()
    await _run(runner, first_metrics, snapshot, {CHAIN: FakeWeb3(eth)})
    _persist_mirrors(runner, snapshot)

    eth._head += advance_head
    snapshot = _snapshot()
    await _run(runner, metrics if metrics is not None else _metrics(), snapshot, {CHAIN: FakeWeb3(eth)})
    _persist_mirrors(runner, snapshot)
    return snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]


@pytest.fixture(autouse=True)
def _clear_caches() -> None:
    clear_provenance_caches()


# --------------------------------------------------------------------------
# Era initialization
# --------------------------------------------------------------------------


class TestEraInitialization:
    @pytest.mark.asyncio
    async def test_pending_while_ledger_empty(self):
        """No anchor tx yet ⇒ pending; the columns keep their current values."""
        runner = FakeRunner(FakeStateManager(ledger=[]))
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(FakeEth(head=100))})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_PENDING
        assert record["cursors"] == {}
        assert metrics.deposits_usd == Decimal("0")
        assert metrics.withdrawals_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_pending_stays_pending_across_cycles(self):
        runner = FakeRunner(FakeStateManager(ledger=[]))
        for _ in range(3):
            metrics, snapshot = _metrics(), _snapshot()
            await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(FakeEth(head=100))})
            _persist_mirrors(runner, snapshot)
            assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["status"] == STATUS_PENDING

    @pytest.mark.asyncio
    async def test_pending_upgrades_to_measured_at_tx1_block(self):
        """The era opens at the block of the deployment's first ledger tx."""
        sm = FakeStateManager(ledger=[])
        runner = FakeRunner(sm)
        eth = FakeEth(head=200, txs={"0xanchor": {"blockNumber": 110}})

        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})
        _persist_mirrors(runner, snapshot)

        sm.ledger = [_ledger_row()]
        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_MEASURED
        assert record["cursors"] == {CHAIN: 110}
        assert record["era_start"] == {CHAIN: 110}
        assert metrics.deposits_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_deposit_before_anchor_is_never_booked(self):
        """THE anchor-absorption test.

        A deposit at block 100 lands before the deployment's first ledger tx
        (block 110). The read side's ``wallet_anchored(tx₁ pre-state)`` already
        contains it, so the producer must never book it — doing so subtracts
        the capital twice and prints a phantom loss.
        """
        sm = FakeStateManager(ledger=[])
        runner = FakeRunner(sm)
        eth = FakeEth(
            head=120,
            logs=[_log(frm=OUTSIDER, to=WALLET, amount=500_000_000, block=100, tx="0xpre")],
            txs={"0xanchor": {"blockNumber": 110}},
        )

        # Cycle 1: no ledger yet -> pending.
        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})
        _persist_mirrors(runner, snapshot)

        # Cycle 2: tx1 exists -> era opens at block 110.
        sm.ledger = [_ledger_row()]
        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})
        _persist_mirrors(runner, snapshot)

        # Cycle 3: first real scan of (110, 130].
        eth._head = 130
        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        assert metrics.deposits_usd == Decimal("0"), "pre-anchor deposit must stay inside the anchor"
        assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["status"] == STATUS_MEASURED

    @pytest.mark.asyncio
    async def test_legacy_deployment_starts_era_now_and_books_nothing(self):
        """No flow-state + non-empty ledger ⇒ era starts at head, not at tx₁."""
        sm = FakeStateManager(ledger=[_ledger_row()])
        runner = FakeRunner(sm)
        eth = FakeEth(
            head=900,
            logs=[_log(frm=OUTSIDER, to=WALLET, amount=500_000_000, block=850, tx="0xold")],
            txs={"0xanchor": {"blockNumber": 110}},
        )

        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_MEASURED
        assert record["cursors"] == {CHAIN: 900}, "legacy era starts NOW"
        assert metrics.deposits_usd == Decimal("0"), "no historical booking on upgrade day"

    @pytest.mark.asyncio
    async def test_legacy_init_preserves_existing_column_values(self):
        sm = FakeStateManager(ledger=[_ledger_row()])
        runner = FakeRunner(sm)
        metrics = _metrics(deposits=Decimal("7.5"), withdrawals=Decimal("2"))
        snapshot = _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(FakeEth(head=900))})

        assert metrics.deposits_usd == Decimal("7.5")
        assert metrics.withdrawals_usd == Decimal("2")


# --------------------------------------------------------------------------
# Accrual
# --------------------------------------------------------------------------


def _measured_runner(
    *,
    cursor: int = 100,
    deposits: str = "0",
    withdrawals: str = "0",
    ledger: list[Any] | None = None,
    chains: dict[str, int] | None = None,
) -> FakeRunner:
    sm = FakeStateManager(ledger=ledger if ledger is not None else [_ledger_row()])
    cursors = chains if chains is not None else {CHAIN: cursor}
    sm.snapshot_mirror = CapitalFlowRecord(
        status=STATUS_MEASURED,
        cursors=dict(cursors),
        era_start=dict(cursors),
        deposits_usd=Decimal(deposits),
        withdrawals_usd=Decimal(withdrawals),
    ).to_record()
    return FakeRunner(sm)


class TestAccrual:
    @pytest.mark.asyncio
    async def test_eoa_deposit_adds_to_deposits(self):
        runner = _measured_runner()
        eth = FakeEth(head=120, logs=[_log(frm=OUTSIDER, to=WALLET, amount=250_000_000, block=110, tx="0xdep")])
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        assert metrics.deposits_usd == Decimal("250")
        assert metrics.withdrawals_usd == Decimal("0")
        assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["cursors"] == {CHAIN: 120}

    @pytest.mark.asyncio
    async def test_eoa_withdrawal_adds_to_withdrawals(self):
        runner = _measured_runner()
        eth = FakeEth(
            head=120,
            logs=[_log(frm=WALLET, to=OUTSIDER, amount=40_000_000, block=110, tx="0xwd")],
            txs={"0xwd": {"from": WALLET}},
        )
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        assert metrics.withdrawals_usd == Decimal("40")
        assert metrics.deposits_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_pull_by_approved_spender_is_not_a_withdrawal(self):
        """tx.from != wallet ⇒ unclassified (a sweep or a theft), never netted out.

        Deferred one cycle by the defer-and-recheck rule, then folded into
        forensics once the ledger still cannot explain it.
        """
        runner = _measured_runner()
        eth = FakeEth(
            head=120,
            logs=[_log(frm=WALLET, to=OUTSIDER, amount=500_000, block=110, tx="0xpull")],
            txs={"0xpull": {"from": OUTSIDER}},
        )
        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})
        _persist_mirrors(runner, snapshot)

        assert metrics.withdrawals_usd == Decimal("0")
        assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["unclassified_out_usd"] == "0", "deferred, not yet judged"
        assert len(snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["pending_unclassified"]) == 1

        eth._head = 140
        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        assert metrics.withdrawals_usd == Decimal("0")
        assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["unclassified_out_usd"] == "0.5"
        assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["pending_unclassified"] == []

    @pytest.mark.asyncio
    async def test_strategy_ledger_tx_is_not_a_flow(self):
        runner = _measured_runner(ledger=[_ledger_row(tx="0xanchor"), _ledger_row(tx="0xswap", seconds=5)])
        eth = FakeEth(head=120, logs=[_log(frm=OUTSIDER, to=WALLET, amount=999_000_000, block=110, tx="0xswap")])
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        assert metrics.deposits_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_accrual_is_cumulative_across_iterations(self):
        runner = _measured_runner()
        eth = FakeEth(head=120, logs=[_log(frm=OUTSIDER, to=WALLET, amount=100_000_000, block=110, tx="0xd1")])

        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})
        _persist_mirrors(runner, snapshot)
        assert metrics.deposits_usd == Decimal("100")

        eth._head = 140
        eth._logs.append(_log(frm=OUTSIDER, to=WALLET, amount=30_000_000, block=130, tx="0xd2"))
        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        assert metrics.deposits_usd == Decimal("130"), "cumulative, and the first deposit is not re-counted"
        assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["cursors"] == {CHAIN: 140}


# --------------------------------------------------------------------------
# Degraded / poisoned paths
# --------------------------------------------------------------------------


class TestDegradedPaths:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", ["live", "paper", "dry_run"])
    async def test_transient_failure_never_raises_and_keeps_cursor(self, mode):
        """A network read is not local-DB integrity: no mode may halt on it."""
        runner = _measured_runner(cursor=100, deposits="12.5")
        eth = FakeEth(head=999_999, logs_raise=ValueError("query returned more than 10000 results"))
        snapshot = _snapshot()

        async def _fake_gas(*args: Any, **kwargs: Any) -> None:
            return None

        def _resolve(client: Any, chain: str, *a: Any, **k: Any) -> FakeWeb3:
            return FakeWeb3(eth)

        runner.state_manager.get_portfolio_metrics = _returns(_metrics())  # type: ignore[attr-defined]
        with (
            patch("almanak.framework.web3.get_gateway_web3", side_effect=_resolve),
            patch("almanak.framework.runner.runner_state._populate_gas_spent_usd", _fake_gas),
            patch(
                "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
                return_value=mode,
            ),
        ):
            metrics = await _build_metrics_for_snapshot(runner, DEPLOYMENT, snapshot)

        assert metrics is not None
        assert metrics.deposits_usd == Decimal("12.5"), "prior projection survives"
        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["cursors"] == {CHAIN: 100}, "cursor unmoved"
        assert record["status"] == STATUS_MEASURED
        assert record["status_detail"] == DETAIL_SCAN_DEFERRED

    @pytest.mark.asyncio
    async def test_range_unmeasurable_poisons_and_advances_cursor(self):
        runner = _measured_runner(cursor=1, deposits="5")
        eth = FakeEth(head=5_000_000)
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_UNMEASURED
        assert record["unmeasured_reason"] == REASON_SCAN_GAP
        assert record["cursors"] == {CHAIN: 5_000_000}
        assert metrics.deposits_usd is None
        assert metrics.withdrawals_usd is None

    @pytest.mark.asyncio
    async def test_unpriceable_flow_poisons(self):
        """A flow we can see but cannot value must poison, never guess."""
        runner = _measured_runner()
        eth = FakeEth(head=120, logs=[_log(token=MYSTERY, frm=OUTSIDER, to=WALLET, amount=1, block=110, tx="0xm")])
        metrics = _metrics()
        snapshot = _snapshot(
            balances=[TokenBalance("MYST", Decimal("1"), Decimal("0"), MYSTERY)],
            prices={f"{CHAIN}:{USDC}": {"price_usd": "1", "symbol": "USDC", "decimals": 6}},
        )

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_UNMEASURED
        assert record["unmeasured_reason"] == REASON_UNPRICEABLE_FLOW
        assert metrics.deposits_usd is None
        # Same forensics/pending contract as the materiality poison: a
        # poisoned era never re-judges pending, so it must not retain any.
        assert record["pending_unclassified"] == []

    @pytest.mark.asyncio
    async def test_unpriceable_poison_folds_resolved_forensics(self):
        """Resolved-pending forensics survive an unpriceable poison (review fix)."""
        runner = _measured_runner()
        runner.state_manager.snapshot_mirror = CapitalFlowRecord(
            status=STATUS_MEASURED,
            cursors={CHAIN: 100},
            era_start={CHAIN: 100},
            deposits_usd=Decimal("0"),
            withdrawals_usd=Decimal("0"),
            pending_unclassified=(
                PendingUnclassified(
                    tx_hash="0xexternal",
                    chain=CHAIN,
                    token_address=USDC,
                    direction="IN",
                    block=90,
                    value_usd=Decimal("7"),
                ),
            ),
        ).to_record()
        # The scan window carries an unpriceable flow (poison trigger) while
        # the prior cycle's deferred external tx survives its ledger recheck.
        eth = FakeEth(head=120, logs=[_log(token=MYSTERY, frm=OUTSIDER, to=WALLET, amount=1, block=110, tx="0xm")])
        metrics = _metrics()
        snapshot = _snapshot(
            balances=[TokenBalance("MYST", Decimal("1"), Decimal("0"), MYSTERY)],
            prices={f"{CHAIN}:{USDC}": {"price_usd": "1", "symbol": "USDC", "decimals": 6}},
        )

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["unmeasured_reason"] == REASON_UNPRICEABLE_FLOW
        assert record["unclassified_in_usd"] == "7"
        assert record["pending_unclassified"] == []

    @pytest.mark.asyncio
    async def test_unmeasured_is_sticky_and_skips_scanning(self):
        runner = _measured_runner()
        runner.state_manager.snapshot_mirror = CapitalFlowRecord(
            status=STATUS_UNMEASURED,
            cursors={CHAIN: 100},
            era_start={CHAIN: 100},
            deposits_usd=None,
            withdrawals_usd=None,
            unmeasured_reason=REASON_SCAN_GAP,
        ).to_record()
        eth = FakeEth(head=200, logs=[_log(frm=OUTSIDER, to=WALLET, amount=100_000_000, block=150, tx="0xd")])
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        assert metrics.deposits_usd is None
        assert eth.get_logs_calls == [], "a poisoned era does not pay for scans"
        assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["unmeasured_reason"] == REASON_SCAN_GAP

    @pytest.mark.asyncio
    async def test_no_gateway_keeps_prior_record(self):
        runner = _measured_runner(deposits="9")
        runner._gateway = None
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_MEASURED
        assert record["status_detail"] == DETAIL_NO_GATEWAY
        assert metrics.deposits_usd == Decimal("9")

    @pytest.mark.asyncio
    async def test_producer_never_raises_on_unexpected_error(self):
        runner = _measured_runner()
        metrics, snapshot = _metrics(deposits=Decimal("3")), _snapshot()

        with patch(
            "almanak.framework.runner.runner_state._advance_capital_flows",
            side_effect=RuntimeError("boom"),
        ):
            await _populate_capital_flows(runner, metrics, snapshot, deployment_id=DEPLOYMENT)

        assert metrics.deposits_usd == Decimal("3")
        assert CAPITAL_FLOWS_KEY not in snapshot.snapshot_metadata


def _returns(value: Any):
    async def _inner(*args: Any, **kwargs: Any) -> Any:
        return value

    return _inner


# --------------------------------------------------------------------------
# Attribution gate
# --------------------------------------------------------------------------


class TestAttributionGate:
    @pytest.mark.asyncio
    async def test_non_canonical_deployment_id_poisons(self):
        runner = _measured_runner()
        metrics, snapshot = _metrics(), _snapshot()

        with patch("almanak.framework.web3.get_gateway_web3", side_effect=AssertionError("must not scan")):
            await _populate_capital_flows(runner, metrics, snapshot, deployment_id="my-strategy")

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["unmeasured_reason"] == REASON_SHARED_WALLET
        assert metrics.deposits_usd is None
        assert metrics.withdrawals_usd is None

    @pytest.mark.asyncio
    async def test_second_deployment_in_backend_poisons(self):
        runner = _measured_runner()
        runner.state_manager.deployment_ids = [DEPLOYMENT, "deployment:ffffffffffff"]
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(FakeEth(head=120))})

        assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["unmeasured_reason"] == REASON_SHARED_WALLET
        assert metrics.deposits_usd is None


# --------------------------------------------------------------------------
# Materiality gate
# --------------------------------------------------------------------------


class TestMaterialityGate:
    @pytest.mark.asyncio
    async def test_just_below_threshold_stays_measured_and_accumulates_forensics(self):
        # NAV 1000 -> threshold max($1, $1) = $1. A $0.90 mint stays measured.
        runner = _measured_runner()
        eth = FakeEth(head=120, logs=[_log(frm=ZERO_ADDRESS, to=WALLET, amount=900_000, block=110, tx="0xmint")])
        record = await _two_cycles(runner, eth)

        assert record["status"] == STATUS_MEASURED
        assert record["unclassified_in_usd"] == "0.9"

    @pytest.mark.asyncio
    async def test_just_above_threshold_poisons_with_forensics(self):
        runner = _measured_runner()
        eth = FakeEth(head=120, logs=[_log(frm=ZERO_ADDRESS, to=WALLET, amount=1_100_000, block=110, tx="0xmint")])
        metrics = _metrics()
        record = await _two_cycles(runner, eth, metrics=metrics)

        assert record["status"] == STATUS_UNMEASURED
        assert record["unmeasured_reason"] == REASON_UNCLASSIFIED_MATERIAL
        assert record["unclassified_in_usd"] == "1.1", "poisoning sums are recorded, not left at 0"
        assert metrics.deposits_usd is None

    @pytest.mark.asyncio
    async def test_threshold_scales_with_nav(self):
        assert materiality_threshold(Decimal("100")) == Decimal("1")
        assert materiality_threshold(Decimal("1000000")) == Decimal("1000")

    @pytest.mark.asyncio
    async def test_contract_transfer_is_unclassified_not_a_deposit(self):
        runner = _measured_runner()
        eth = FakeEth(
            head=120,
            logs=[_log(frm=CONTRACT, to=WALLET, amount=500_000, block=110, tx="0xc")],
            codes={CONTRACT: "0x6080604052"},
        )
        metrics = _metrics()
        record = await _two_cycles(runner, eth, metrics=metrics)

        assert metrics.deposits_usd == Decimal("0")
        assert record["unclassified_in_usd"] == "0.5"


# --------------------------------------------------------------------------
# Defer-and-recheck (own-tx ledger race)
# --------------------------------------------------------------------------


class TestDeferAndRecheck:
    @pytest.mark.asyncio
    async def test_own_tx_missing_from_ledger_page_does_not_poison(self):
        """THE run-2 race repro.

        The strategy's own SUPPLY tx is scanned in the same instant its ledger
        row is being written, so the row is absent from the page fetched this
        cycle and both Transfer legs classify UNCLASSIFIED. Gating there
        poisoned a healthy era. Deferring one cycle — by which time the row is
        durably visible — must resolve it to "ours" and book nothing.
        """
        sm = FakeStateManager(ledger=[_ledger_row()])
        sm.snapshot_mirror = CapitalFlowRecord(
            status=STATUS_MEASURED, cursors={CHAIN: 100}, era_start={CHAIN: 100}
        ).to_record()
        runner = FakeRunner(sm)
        # Two legs of the strategy's own tx, well above the $1 gate.
        eth = FakeEth(
            head=120,
            logs=[
                _log(frm=CONTRACT, to=WALLET, amount=800_000_000, block=110, index=0, tx="0x160a765a"),
                _log(frm=WALLET, to=CONTRACT, amount=800_000_000, block=110, index=1, tx="0x160a765a"),
            ],
            codes={CONTRACT: "0x6080604052"},
            txs={"0x160a765a": {"from": WALLET}},
        )

        # Cycle 1: ledger page does NOT yet contain the tx -> deferred.
        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})
        _persist_mirrors(runner, snapshot)
        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_MEASURED, "must not poison on first sighting"
        assert len(record["pending_unclassified"]) == 2

        # Cycle 2: the row is durably visible now.
        sm.ledger = [_ledger_row(), _ledger_row(tx="0x160a765a", seconds=5)]
        eth._head = 140
        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_MEASURED, "own tx must never poison the era"
        assert record["pending_unclassified"] == [], "deferral drained"
        assert record["unclassified_in_usd"] == "0", "our own tx is not forensic noise"
        assert record["unclassified_out_usd"] == "0"
        assert metrics.deposits_usd == Decimal("0")
        assert metrics.withdrawals_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_genuine_external_flow_still_poisons_at_recheck(self):
        """Absent from the ledger at N AND N+1 ⇒ really external ⇒ poison."""
        runner = _measured_runner()
        eth = FakeEth(
            head=120,
            logs=[_log(frm=CONTRACT, to=WALLET, amount=50_000_000, block=110, tx="0xext")],
            codes={CONTRACT: "0x6080604052"},
        )
        metrics = _metrics()

        with patch("almanak.framework.runner.runner_state.logger") as log:
            record = await _two_cycles(runner, eth, metrics=metrics)

        assert record["status"] == STATUS_UNMEASURED
        assert record["unmeasured_reason"] == REASON_UNCLASSIFIED_MATERIAL
        assert record["unclassified_in_usd"] == "50"
        assert metrics.deposits_usd is None

        warnings = [c for c in log.warning.call_args_list if c.args and c.args[0] == "capital_flows: era poisoned"]
        assert len(warnings) == 1, "exactly one structured WARNING names the cause"
        kwargs = warnings[0].kwargs
        assert kwargs["reason"] == REASON_UNCLASSIFIED_MATERIAL
        assert kwargs["unclassified_in_usd"] == "50"
        assert kwargs["tx_hashes"] == ["0xext"]

    @pytest.mark.asyncio
    async def test_sub_threshold_external_flow_survives_recheck(self):
        runner = _measured_runner()
        eth = FakeEth(
            head=120,
            logs=[_log(frm=CONTRACT, to=WALLET, amount=250_000, block=110, tx="0xsmall")],
            codes={CONTRACT: "0x6080604052"},
        )
        record = await _two_cycles(runner, eth)

        assert record["status"] == STATUS_MEASURED
        assert record["unclassified_in_usd"] == "0.25"
        assert record["pending_unclassified"] == []

    @pytest.mark.asyncio
    async def test_eoa_flows_book_in_the_same_cycle_without_deferral(self):
        """Deferral must not add latency to real deposits/withdrawals."""
        runner = _measured_runner()
        eth = FakeEth(
            head=120,
            logs=[
                _log(frm=OUTSIDER, to=WALLET, amount=60_000_000, block=110, index=0, tx="0xin"),
                _log(frm=WALLET, to=OUTSIDER, amount=20_000_000, block=111, index=1, tx="0xout"),
            ],
            txs={"0xout": {"from": WALLET}},
        )
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        assert metrics.deposits_usd == Decimal("60")
        assert metrics.withdrawals_usd == Decimal("20")
        assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["pending_unclassified"] == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("snapshot_is_newer", [True, False])
    async def test_pending_survives_restart_and_is_rechecked(self, snapshot_is_newer):
        """A restart between defer and recheck must not lose the deferral."""
        sm = FakeStateManager(ledger=[_ledger_row(), _ledger_row(tx="0xown", seconds=5)])
        deferred = CapitalFlowRecord(
            status=STATUS_MEASURED,
            cursors={CHAIN: 120},
            era_start={CHAIN: 100},
            pending_unclassified=(
                PendingUnclassified(
                    tx_hash="0xown",
                    chain=CHAIN,
                    token_address=USDC,
                    direction="IN",
                    block=110,
                    value_usd=Decimal("900"),
                ),
            ),
        ).to_record()
        stale = CapitalFlowRecord(status=STATUS_MEASURED, cursors={CHAIN: 100}, era_start={CHAIN: 100}).to_record()

        sm.snapshot_mirror = deferred if snapshot_is_newer else stale
        sm.state_mirror[CAPITAL_FLOWS_KEY] = json.dumps(stale if snapshot_is_newer else deferred)
        runner = FakeRunner(sm)

        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(FakeEth(head=140))})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_MEASURED, "$900 deferral resolved to our own tx after restart"
        assert record["pending_unclassified"] == []
        assert record["unclassified_in_usd"] == "0"

    @pytest.mark.asyncio
    async def test_pending_overflow_poisons_with_its_own_reason(self):
        runner = _measured_runner()
        # Each transfer is $0.001 — far below the gate, so only the bound fires.
        logs = [
            _log(frm=CONTRACT, to=WALLET, amount=1_000, block=101 + i, index=i, tx=f"0x{i:04x}")
            for i in range(MAX_PENDING_UNCLASSIFIED + 1)
        ]
        eth = FakeEth(head=400, logs=logs, codes={CONTRACT: "0x6080604052"})
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_UNMEASURED
        assert record["unmeasured_reason"] == REASON_PENDING_OVERFLOW
        assert record["pending_unclassified"] == [], "bounded: the record cannot grow without limit"
        assert metrics.deposits_usd is None

    def test_pending_entries_round_trip_through_the_record(self):
        entry = PendingUnclassified(
            tx_hash="0xabc", chain=CHAIN, token_address=USDC, direction="OUT", block=7, value_usd=Decimal("1.5")
        )
        unpriced = PendingUnclassified(
            tx_hash="0xdef", chain=CHAIN, token_address=MYSTERY, direction="IN", block=8, value_usd=None
        )
        raw = CapitalFlowRecord(status=STATUS_MEASURED, pending_unclassified=(entry, unpriced)).to_record()

        restored = CapitalFlowRecord.from_record(json.loads(json.dumps(raw)))

        assert restored is not None
        assert restored.pending_unclassified == (entry, unpriced)
        assert restored.pending_unclassified[1].value_usd is None, "unpriceable stays None, never 0"


# --------------------------------------------------------------------------
# Gateway row shape (the managed-gateway path)
# --------------------------------------------------------------------------


def _measured_mirror(cursor: int = 100) -> dict[str, Any]:
    return CapitalFlowRecord(
        status=STATUS_MEASURED, cursors={CHAIN: cursor}, era_start={CHAIN: cursor}
    ).to_record()


class TestGatewayLedgerRowShape:
    """Regression cover for real-fork proof run 3.

    Every earlier fake served *attribute*-style rows through
    ``get_ledger_entries``. A managed-gateway run has no such method: it
    serves dicts from ``read_ledger_entries_measured``, and that projection
    silently dropped ``tx_hash``. The exclusion set was empty on every build,
    so the strategy's own trades scanned as unclassified external flows.
    """

    @pytest.mark.asyncio
    async def test_own_tx_excluded_via_gateway_dict_rows(self):
        sm = FakeGatewayStateManager(rows=[_gateway_ledger_row(tx="0x160a765a")])
        sm.snapshot_mirror = _measured_mirror()
        runner = FakeRunner(sm)
        eth = FakeEth(
            head=120,
            logs=[
                _log(frm=CONTRACT, to=WALLET, amount=800_000_000, block=110, index=0, tx="0x160a765a"),
                _log(frm=WALLET, to=CONTRACT, amount=800_000_000, block=110, index=1, tx="0x160a765a"),
            ],
            codes={CONTRACT: "0x6080604052"},
            txs={"0x160a765a": {"from": WALLET}},
        )
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_MEASURED, "own tx must be excluded, not poison the era"
        assert record["pending_unclassified"] == [], "STRATEGY_TX never even reaches the deferral"
        assert record["unclassified_in_usd"] == "0"
        assert metrics.deposits_usd == Decimal("0")
        assert metrics.withdrawals_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_run3_repro_projection_without_tx_hash_self_poisons(self):
        """Pin the failure mode itself, so the bug can never return silently.

        With ``tx_hash`` absent from the row the exclusion set is empty, the
        strategy's own SUPPLY legs classify UNCLASSIFIED, and the era poisons.
        This asserts the BROKEN behaviour on a deliberately broken row — it is
        the control that proves the sibling tests above are discriminating.
        """
        sm = FakeGatewayStateManager(rows=[_gateway_ledger_row(tx="0x160a765a", with_tx_hash=False)])
        sm.snapshot_mirror = _measured_mirror()
        runner = FakeRunner(sm)
        eth = FakeEth(
            head=120,
            logs=[_log(frm=CONTRACT, to=WALLET, amount=800_000_000, block=110, tx="0x160a765a")],
            codes={CONTRACT: "0x6080604052"},
            txs={"0x160a765a": {"from": WALLET}},
        )
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert len(record["pending_unclassified"]) == 1, "own tx wrongly deferred as external"

    @pytest.mark.asyncio
    async def test_external_flow_still_seen_through_gateway_rows(self):
        """The exclusion set must be tx-scoped, not a blanket suppressor."""
        sm = FakeGatewayStateManager(rows=[_gateway_ledger_row(tx="0x160a765a")])
        sm.snapshot_mirror = _measured_mirror()
        runner = FakeRunner(sm)
        eth = FakeEth(head=120, logs=[_log(frm=OUTSIDER, to=WALLET, amount=75_000_000, block=110, tx="0xwire")])
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        assert metrics.deposits_usd == Decimal("75")

    @pytest.mark.asyncio
    async def test_int_epoch_timestamps_drive_the_anchor_path(self):
        """``_ledger_sort_key`` must order int epochs, not just datetimes."""
        sm = FakeGatewayStateManager(
            rows=[
                _gateway_ledger_row(tx="0xlater", epoch=1_784_000_500),
                _gateway_ledger_row(tx="0xoldest", epoch=1_784_000_000),
            ]
        )
        sm.snapshot_mirror = CapitalFlowRecord(status=STATUS_PENDING).to_record()
        runner = FakeRunner(sm)
        eth = FakeEth(head=200, txs={"0xoldest": {"blockNumber": 110}, "0xlater": {"blockNumber": 180}})
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_MEASURED
        assert record["era_start"] == {CHAIN: 110}, "anchor is the OLDEST row by epoch"

    def test_ledger_sort_key_orders_iso_strings(self):
        """ISO-8601 string timestamps must sort, not collapse to 0.0 (review fix)."""
        from almanak.framework.runner.runner_state import _ledger_sort_key

        older = {"timestamp": "2026-07-19T12:00:00+00:00"}
        newer = {"timestamp": "2026-07-19T13:00:00+00:00"}
        assert _ledger_sort_key(older) < _ledger_sort_key(newer)
        assert _ledger_sort_key(older) > 0.0
        assert _ledger_sort_key({"timestamp": "not-a-date"}) == 0.0

    @pytest.mark.asyncio
    async def test_unmeasured_gateway_read_leaves_the_record_untouched(self):
        """``measured=False`` ⇒ rows unknown ⇒ never mistaken for an empty ledger."""
        sm = FakeGatewayStateManager(rows=[], measured=False)
        sm.snapshot_mirror = CapitalFlowRecord(
            status=STATUS_MEASURED,
            cursors={CHAIN: 100},
            era_start={CHAIN: 100},
            deposits_usd=Decimal("42"),
        ).to_record()
        runner = FakeRunner(sm)
        eth = FakeEth(head=200, logs=[_log(frm=OUTSIDER, to=WALLET, amount=9_000_000, block=150, tx="0xd")])
        metrics, snapshot = _metrics(), _snapshot()

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["cursors"] == {CHAIN: 100}, "cursor unmoved"
        assert metrics.deposits_usd == Decimal("42"), "prior projection preserved"
        assert eth.get_logs_calls == [], "an unknown ledger must not be scanned against"


# --------------------------------------------------------------------------
# Recovery
# --------------------------------------------------------------------------


class TestRecovery:
    def test_recover_takes_higher_cursor_wholesale(self):
        newer = CapitalFlowRecord(
            status=STATUS_MEASURED, cursors={CHAIN: 200}, deposits_usd=Decimal("15")
        ).to_record()
        older = CapitalFlowRecord(status=STATUS_MEASURED, cursors={CHAIN: 100}, deposits_usd=Decimal("5")).to_record()

        for candidates in ([newer, older], [older, newer]):
            record = recover_record(candidates)
            assert record is not None
            assert record.cursors == {CHAIN: 200}
            assert record.deposits_usd == Decimal("15"), "fields never mixed across mirrors"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("snapshot_is_newer", [True, False])
    async def test_crash_orderings_do_not_double_count(self, snapshot_is_newer):
        """One mirror is a cycle behind after a crash; neither ordering re-books."""
        sm = FakeStateManager(ledger=[_ledger_row()])
        ahead = CapitalFlowRecord(
            status=STATUS_MEASURED,
            cursors={CHAIN: 120},
            era_start={CHAIN: 100},
            deposits_usd=Decimal("100"),
        ).to_record()
        behind = CapitalFlowRecord(
            status=STATUS_MEASURED,
            cursors={CHAIN: 100},
            era_start={CHAIN: 100},
            deposits_usd=Decimal("0"),
        ).to_record()

        sm.snapshot_mirror = ahead if snapshot_is_newer else behind
        sm.state_mirror[CAPITAL_FLOWS_KEY] = json.dumps(behind if snapshot_is_newer else ahead)
        runner = FakeRunner(sm)

        eth = FakeEth(head=140, logs=[_log(frm=OUTSIDER, to=WALLET, amount=100_000_000, block=110, tx="0xd1")])
        metrics, snapshot = _metrics(), _snapshot()
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(eth)})

        assert metrics.deposits_usd == Decimal("100"), "the block-110 deposit is booked exactly once"

    def test_unreadable_or_foreign_schema_mirror_is_ignored(self):
        assert recover_record([None, "garbage", {"schema_version": 999}]) is None
        good = CapitalFlowRecord(status=STATUS_MEASURED, cursors={CHAIN: 5}).to_record()
        assert recover_record([{"schema_version": 999}, good]) is not None

    @pytest.mark.asyncio
    async def test_strategy_state_mirror_is_written_without_status_detail(self):
        sm = FakeStateManager()
        runner = FakeRunner(sm)
        snapshot = _snapshot()
        snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY] = {
            **CapitalFlowRecord(status=STATUS_MEASURED, cursors={CHAIN: 7}).to_record(),
            "status_detail": DETAIL_SCAN_DEFERRED,
        }

        await _write_valuation_into_strategy_state(runner, DEPLOYMENT, snapshot)

        stored = json.loads(sm.saved_state.state[CAPITAL_FLOWS_KEY])
        assert stored["cursors"] == {CHAIN: 7}
        assert stored["schema_version"] == SCHEMA_VERSION
        assert "status_detail" not in stored


# --------------------------------------------------------------------------
# Multi-chain
# --------------------------------------------------------------------------


class TestMultiChain:
    @pytest.mark.asyncio
    async def test_per_chain_cursors_advance_independently(self):
        runner = _measured_runner(
            chains={CHAIN: 100, OTHER_CHAIN: 500},
            ledger=[_ledger_row(), _ledger_row(tx="0xb", chain=OTHER_CHAIN, seconds=5)],
        )
        arb = FakeEth(head=120, logs=[_log(frm=OUTSIDER, to=WALLET, amount=10_000_000, block=110, tx="0xa1")])
        base = FakeEth(head=600, logs=[_log(frm=OUTSIDER, to=WALLET, amount=5_000_000, block=550, tx="0xb1")])
        metrics = _metrics()
        snapshot = _snapshot(
            prices={
                f"{CHAIN}:{USDC}": {"price_usd": "1", "symbol": "USDC", "decimals": 6},
                f"{OTHER_CHAIN}:{USDC}": {"price_usd": "1", "symbol": "USDC", "decimals": 6},
            }
        )

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(arb), OTHER_CHAIN: FakeWeb3(base)})

        assert metrics.deposits_usd == Decimal("15"), "both chains contribute"
        assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["cursors"] == {CHAIN: 120, OTHER_CHAIN: 600}

    @pytest.mark.asyncio
    async def test_unscannable_second_chain_poisons(self):
        runner = _measured_runner(
            chains={CHAIN: 100, OTHER_CHAIN: 500},
            ledger=[_ledger_row(), _ledger_row(tx="0xb", chain=OTHER_CHAIN, seconds=5)],
        )
        metrics, snapshot = _metrics(), _snapshot()

        # Only the primary chain has a handle; ``base`` raises on resolution.
        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(FakeEth(head=120))})

        record = snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]
        assert record["status"] == STATUS_UNMEASURED
        assert record["unmeasured_reason"] == REASON_CHAIN_UNSCANNABLE
        assert metrics.deposits_usd is None

    @pytest.mark.asyncio
    async def test_new_chain_joins_at_head_without_booking_history(self):
        runner = _measured_runner(
            chains={CHAIN: 100},
            ledger=[_ledger_row(), _ledger_row(tx="0xb", chain=OTHER_CHAIN, seconds=5)],
        )
        base = FakeEth(head=600, logs=[_log(frm=OUTSIDER, to=WALLET, amount=99_000_000, block=550, tx="0xb1")])
        metrics = _metrics()
        snapshot = _snapshot(
            prices={
                f"{CHAIN}:{USDC}": {"price_usd": "1", "symbol": "USDC", "decimals": 6},
                f"{OTHER_CHAIN}:{USDC}": {"price_usd": "1", "symbol": "USDC", "decimals": 6},
            }
        )

        await _run(runner, metrics, snapshot, {CHAIN: FakeWeb3(FakeEth(head=120)), OTHER_CHAIN: FakeWeb3(base)})

        assert metrics.deposits_usd == Decimal("0")
        assert snapshot.snapshot_metadata[CAPITAL_FLOWS_KEY]["cursors"] == {CHAIN: 120, OTHER_CHAIN: 600}


# --------------------------------------------------------------------------
# Projection contract
# --------------------------------------------------------------------------


class TestProjection:
    @pytest.mark.parametrize(
        "status,expected_none",
        [(STATUS_MEASURED, False), (STATUS_UNMEASURED, True), (STATUS_PENDING, True)],
    )
    def test_columns_are_none_iff_not_measured(self, status, expected_none):
        record = CapitalFlowRecord(status=status, deposits_usd=Decimal("4"), withdrawals_usd=Decimal("1"))
        deposits, withdrawals = project_columns(record)
        assert (deposits is None) is expected_none
        assert (withdrawals is None) is expected_none

    @pytest.mark.asyncio
    async def test_hook_runs_on_the_baseline_branch_too(self):
        """Both branches of ``_build_metrics_for_snapshot`` call the producer."""
        runner = _measured_runner()
        runner.state_manager.get_portfolio_metrics = _returns(None)  # type: ignore[attr-defined]
        runner.state_manager.sum_ledger_gas_usd = _returns(Decimal("0"))  # type: ignore[attr-defined]
        eth = FakeEth(head=120, logs=[_log(frm=OUTSIDER, to=WALLET, amount=25_000_000, block=110, tx="0xd")])
        snapshot = _snapshot()

        def _resolve(client: Any, chain: str, *a: Any, **k: Any) -> FakeWeb3:
            return FakeWeb3(eth)

        with (
            patch("almanak.framework.web3.get_gateway_web3", side_effect=_resolve),
            patch(
                "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
                return_value="paper",
            ),
        ):
            metrics = await _build_metrics_for_snapshot(runner, DEPLOYMENT, snapshot)

        assert metrics is not None
        assert metrics.deposits_usd == Decimal("25")
