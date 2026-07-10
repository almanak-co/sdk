"""VIB-5666 — vault settlement → commit/accounting pipeline.

Covers the three layers of the settlement-commit vertical:

1. ``settlement_handler.handle_settlement`` — ledger/outbox dicts → typed
   :class:`SettlementAccountingEvent`, incl. Empty ≠ Zero + capital-event
   inertness (no PnL fields; ``compute_position_pnl`` ignores the event).
2. ``settlement_commit.commit_settlement_intent`` — the runner-owned pipeline:
   synthetic intent shape, ``extracted_data`` stamping, ledger → outbox routing,
   and the loud-but-never-raise degraded contract.
3. ``VaultLifecycleManager`` wiring — a settlement cycle drives the injected
   commit for each leg; a degraded commit never blocks the state machine; and a
   manager with no commit callable (non-vault / legacy) executes zero new code.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.core.models.config import VaultVersion
from almanak.framework.accounting.category_handlers.settlement_handler import handle_settlement
from almanak.framework.accounting.models import AccountingConfidence
from almanak.framework.accounting.position_pnl import compute_position_pnl
from almanak.framework.accounting.settlement_accounting import SettlementAccountingEvent
from almanak.framework.runner.settlement_commit import (
    SettlementCommitOutcome,
    commit_settlement_intent,
)
from almanak.framework.vault.config import SettlementPhase, VaultConfig, VaultState
from almanak.framework.vault.lifecycle import VaultLifecycleManager

# ── Mock harness (self-contained — does not import sibling test modules) ─────


def _make_config(**overrides) -> VaultConfig:
    defaults = {
        "vault_address": "0x1111111111111111111111111111111111111111",
        "valuator_address": "0x3333333333333333333333333333333333333333",
        "underlying_token": "USDC",
        "settlement_interval_minutes": 60,
        "version": VaultVersion.V0_5_0,
    }
    defaults.update(overrides)
    return VaultConfig(**defaults)


def _make_strategy(chain: str = "ethereum", wallet_address: str = "0x3333333333333333333333333333333333333333"):
    strategy = MagicMock()
    strategy.chain = chain
    strategy.wallet_address = wallet_address
    return strategy


def _make_market(underlying_price: Decimal = Decimal("1.0"), total_portfolio_usd: Decimal = Decimal("10")):
    market = MagicMock()
    market.price.return_value = underlying_price
    market.total_portfolio_usd.return_value = total_portfolio_usd
    return market


def _make_manager(vault_config: VaultConfig | None = None, vault_state: VaultState | None = None):
    config = vault_config or _make_config()
    sdk = MagicMock()
    adapter = MagicMock()
    orchestrator = MagicMock()
    orchestrator.execute = AsyncMock()
    sdk.verify_version.return_value = None
    sdk.get_valuation_manager.return_value = config.valuator_address
    sdk.get_curator.return_value = "0x3333333333333333333333333333333333333333"
    sdk.has_live_proposal.return_value = True
    sdk.get_silo_address.return_value = "0x2222222222222222222222222222222222222222"
    sdk.get_underlying_balance.return_value = 0
    sdk.get_total_assets.return_value = 10**30
    sdk.get_pending_deposits.return_value = 0
    manager = VaultLifecycleManager(
        vault_config=config,
        vault_sdk=sdk,
        vault_adapter=adapter,
        execution_orchestrator=orchestrator,
        deployment_id="test-strategy-1",
    )
    if vault_state is not None:
        manager._vault_state = vault_state
    return manager


# ── Canonical ledger/outbox row shape produced by the commit pipeline ────────


def _ledger_row(*, settlement: dict | None, intent_type: str = "SETTLE_DEPOSIT") -> dict:
    extracted = json.dumps({"settlement": settlement}) if settlement is not None else ""
    return {
        "id": "ledger-1",
        "deployment_id": "strat-1",
        "cycle_id": "settlement-3",
        "execution_mode": "live",
        "chain": "ethereum",
        "protocol": "lagoon",
        "tx_hash": "0x" + "a" * 64,
        "timestamp": "2026-07-09T00:00:00+00:00",
        "intent_type": intent_type,
        "token_in": "USDC",
        "amount_in": "5",
        "extracted_data_json": extracted,
    }


def _outbox_row() -> dict:
    return {
        "wallet_address": "0x" + "3" * 40,
        "position_key": "settlement:lagoon:ethereum:0x333:0xvault",
        "market_id": "0xvault",
    }


# ══════════════════════════════════════════════════════════════════════════════
# Layer 1 — settlement_handler
# ══════════════════════════════════════════════════════════════════════════════


class TestSettlementHandler:
    def test_deposit_event_from_rows_measured(self):
        settlement = {
            "leg": "deposit",
            "assets": "5",
            "shares": "4.9",
            "new_total_assets": "10",
            "fee_shares": None,
            "assets_usd": "5",
            "epoch_id": 3,
        }
        evt = handle_settlement(_outbox_row(), _ledger_row(settlement=settlement))
        assert isinstance(evt, SettlementAccountingEvent)
        assert evt.event_type == "SETTLE_DEPOSIT"
        assert evt.assets_delta == Decimal("5")
        assert evt.shares_delta == Decimal("4.9")
        assert evt.new_total_assets == Decimal("10")
        assert evt.assets_usd == Decimal("5")
        assert evt.fee_shares is None  # Empty ≠ Zero — unmeasured, not 0
        assert evt.epoch_id == 3
        assert evt.vault_address == "0xvault"
        assert evt.asset_token == "USDC"
        assert evt.confidence == AccountingConfidence.HIGH
        assert evt.unavailable_reason == ""

    def test_redeem_event_type(self):
        settlement = {"leg": "redeem", "assets": "2", "shares": "2", "epoch_id": 4}
        evt = handle_settlement(_outbox_row(), _ledger_row(settlement=settlement, intent_type="SETTLE_REDEEM"))
        assert evt is not None
        assert evt.event_type == "SETTLE_REDEEM"
        assert evt.assets_delta == Decimal("2")

    def test_empty_not_zero_missing_deltas(self):
        """A missing (None) receipt leg → unmeasured (None), NEVER Decimal('0'),
        and confidence degrades to ESTIMATED with an audit reason."""
        settlement = {"leg": "deposit", "assets": None, "shares": None, "epoch_id": 3}
        evt = handle_settlement(_outbox_row(), _ledger_row(settlement=settlement))
        assert evt is not None
        assert evt.assets_delta is None
        assert evt.shares_delta is None
        assert evt.confidence == AccountingConfidence.ESTIMATED
        assert "assets_delta" in evt.unavailable_reason
        assert "shares_delta" in evt.unavailable_reason

    def test_absent_extracted_data_degrades_not_crashes(self):
        evt = handle_settlement(_outbox_row(), _ledger_row(settlement=None))
        assert evt is not None
        assert evt.assets_delta is None
        assert evt.confidence == AccountingConfidence.ESTIMATED

    def test_non_settlement_intent_returns_none(self):
        row = _ledger_row(settlement={"leg": "deposit"}, intent_type="SWAP")
        assert handle_settlement(_outbox_row(), row) is None

    def test_payload_has_no_pnl_fields(self):
        """Capital-event discipline: the payload must carry no PnL/return field, so
        no fold can read a settlement as profit/loss."""
        settlement = {"leg": "deposit", "assets": "5", "shares": "5", "epoch_id": 1}
        evt = handle_settlement(_outbox_row(), _ledger_row(settlement=settlement))
        payload = json.loads(evt.to_payload_json())
        for forbidden in ("principal_delta_usd", "realized_pnl_usd", "cost_basis_usd", "yield_usd"):
            assert forbidden not in payload
        # Version stamps present.
        assert payload["schema_version"] == 1
        assert payload["primitive_version"] == 1


class TestCapitalEventInertness:
    def test_compute_position_pnl_ignores_settlement(self):
        """A SETTLE_DEPOSIT / SETTLE_REDEEM accounting row contributes ZERO to
        cost basis and realized PnL — a depositor deposit is not profit, a
        redemption is not a loss."""
        settlement = {"leg": "deposit", "assets": "1000", "shares": "1000", "epoch_id": 1}
        evt = handle_settlement(_outbox_row(), _ledger_row(settlement=settlement))
        events = [
            {
                "event_type": "SETTLE_DEPOSIT",
                "timestamp": "2026-07-09T00:00:00+00:00",
                "ledger_entry_id": "ledger-1",
                "payload_json": evt.to_payload_json(),
            },
            {
                "event_type": "SETTLE_REDEEM",
                "timestamp": "2026-07-09T01:00:00+00:00",
                "ledger_entry_id": "ledger-2",
                "payload_json": evt.to_payload_json(),
            },
        ]
        summary = compute_position_pnl(events)
        assert summary is not None
        assert summary.cost_basis_usd == Decimal("0")
        assert summary.realized_pnl_usd == Decimal("0")


# ══════════════════════════════════════════════════════════════════════════════
# Layer 2 — commit_settlement_intent
# ══════════════════════════════════════════════════════════════════════════════


class _FakeRunner:
    def __init__(self, *, ledger_raises: bool = False):
        self.config = SimpleNamespace(chain="ethereum")
        self.ledger_calls: list = []
        self.outbox_calls: list = []
        self._ledger_raises = ledger_raises

    async def _write_ledger_entry(self, strategy, intent, **kwargs):
        self.ledger_calls.append((intent, kwargs))
        if self._ledger_raises:
            raise RuntimeError("ledger boom")
        return "ledger-1"

    async def _write_outbox_and_fire_processor(self, strategy, intent, ledger_entry_id):
        self.outbox_calls.append((intent, ledger_entry_id))


def _fake_result():
    return SimpleNamespace(
        transaction_results=[SimpleNamespace(tx_hash="0x" + "b" * 64)],
        extracted_data=None,
        total_gas_cost_wei=0,
    )


def _fake_strategy():
    return SimpleNamespace(
        deployment_id="strat-1", chain="ethereum", wallet_address="0x" + "3" * 40
    )


class TestCommitSettlementIntent:
    pytestmark = pytest.mark.asyncio

    async def test_deposit_full_pipeline(self):
        runner = _FakeRunner()
        result = _fake_result()
        outcome = await commit_settlement_intent(
            runner,
            _fake_strategy(),
            leg="deposit",
            execution_result=result,
            settlement_cycle_id="settlement-1",
            vault_address="0xVAULT",
            underlying_token="USDC",
            assets_raw=5_000_000,  # 5 USDC @ 6 decimals
            shares_raw=5_000_000_000_000_000_000,  # 5 shares @ 18 decimals
            new_total_assets_raw=10_000_000,
            epoch_id=1,
            underlying_decimals=6,
            share_decimals=18,
            underlying_price=Decimal("1"),
        )
        assert isinstance(outcome, SettlementCommitOutcome)
        assert outcome.ledger_entry_id == "ledger-1"
        assert outcome.accounting_degraded is False

        # Synthetic intent shape drives the ledger row correctly.
        intent, kwargs = runner.ledger_calls[0]
        assert intent.intent_type == "SETTLE_DEPOSIT"
        assert intent.protocol == "lagoon"
        assert intent.vault_address == "0xVAULT"
        assert intent.from_token == "USDC"
        assert intent.amount == Decimal("5")
        assert kwargs["emit_position_event"] is False
        assert kwargs["success"] is True

        # Human-unit settlement outputs stamped onto extracted_data.
        stamped = result.extracted_data["settlement"]
        assert stamped["assets"] == "5"
        assert stamped["shares"] == "5"
        assert stamped["new_total_assets"] == "10"
        assert stamped["assets_usd"] == "5"
        assert stamped["epoch_id"] == 1

        # Outbox+fire runs for the accounting leg.
        assert runner.outbox_calls == [(intent, "ledger-1")]

    async def test_propose_leg_books_ledger_only(self):
        """The propose leg moves no capital — a ledger row (gas/tx) but NO
        outbox/accounting event, and no settlement extracted_data stamp."""
        runner = _FakeRunner()
        result = _fake_result()
        outcome = await commit_settlement_intent(
            runner,
            _fake_strategy(),
            leg="propose",
            execution_result=result,
            settlement_cycle_id="settlement-1",
            vault_address="0xVAULT",
            underlying_token="USDC",
            epoch_id=1,
            underlying_decimals=6,
            share_decimals=18,
        )
        assert outcome.ledger_entry_id == "ledger-1"
        assert outcome.accounting_degraded is False
        intent, _ = runner.ledger_calls[0]
        assert intent.intent_type == "SETTLE_PROPOSE"
        assert not hasattr(intent, "amount")  # no asset moved
        assert runner.outbox_calls == []  # NO accounting event for propose
        assert result.extracted_data is None  # not stamped

    async def test_empty_not_zero_unknown_decimals(self):
        """Unknown decimals → human amount is unmeasured (None), never a
        fabricated Decimal('0')."""
        runner = _FakeRunner()
        result = _fake_result()
        await commit_settlement_intent(
            runner,
            _fake_strategy(),
            leg="deposit",
            execution_result=result,
            settlement_cycle_id="settlement-1",
            vault_address="0xVAULT",
            underlying_token="USDC",
            assets_raw=5_000_000,
            shares_raw=5,
            epoch_id=1,
            underlying_decimals=None,  # unresolved
            share_decimals=None,
        )
        stamped = result.extracted_data["settlement"]
        assert stamped["assets"] is None
        assert stamped["shares"] is None

    async def test_ledger_failure_degrades_never_raises(self):
        """A ledger-write failure is captured into accounting_degraded and the
        outbox step is skipped — the pipeline NEVER raises (the on-chain settle
        already stands)."""
        runner = _FakeRunner(ledger_raises=True)
        result = _fake_result()
        outcome = await commit_settlement_intent(
            runner,
            _fake_strategy(),
            leg="deposit",
            execution_result=result,
            settlement_cycle_id="settlement-1",
            vault_address="0xVAULT",
            underlying_token="USDC",
            assets_raw=5_000_000,
            shares_raw=5_000_000_000_000_000_000,
            epoch_id=1,
            underlying_decimals=6,
            share_decimals=18,
        )
        assert outcome.ledger_entry_id is None
        assert outcome.accounting_degraded is True
        assert outcome.degraded_reason is not None
        assert runner.outbox_calls == []  # no ledger id → no outbox

    async def test_unknown_leg_degrades(self):
        runner = _FakeRunner()
        outcome = await commit_settlement_intent(
            runner,
            _fake_strategy(),
            leg="bogus",
            execution_result=_fake_result(),
            settlement_cycle_id="settlement-1",
            vault_address="0xVAULT",
            underlying_token="USDC",
        )
        assert outcome.accounting_degraded is True
        assert runner.ledger_calls == []


# ══════════════════════════════════════════════════════════════════════════════
# Layer 3 — VaultLifecycleManager wiring
# ══════════════════════════════════════════════════════════════════════════════


def _idle_state() -> VaultState:
    return VaultState(
        initialized=True,
        last_total_assets=10_000_000,
        settlement_phase=SettlementPhase.IDLE,
        last_valuation_time=datetime.now(UTC) - timedelta(hours=2),
        last_settlement_epoch=2,
    )


def _wire_strategy(manager):
    strategy = _make_strategy(wallet_address="0x3333333333333333333333333333333333333333")
    market = _make_market(underlying_price=Decimal("1.0"), total_portfolio_usd=Decimal("10"))
    strategy.create_market_snapshot.return_value = market
    strategy.valuate.return_value = Decimal("10")
    manager._execution_orchestrator.execute = AsyncMock(
        return_value=SimpleNamespace(success=True, error=None, receipt={}, transaction_results=[])
    )
    # deterministic decimals so assets scale without a gateway read
    manager._settlement_decimals = (6, 18)
    return strategy


class TestLifecycleWiring:
    pytestmark = pytest.mark.asyncio

    async def test_cycle_drives_commit_per_leg(self):
        manager = _make_manager(vault_config=_make_config(auto_settle_redeems=False), vault_state=_idle_state())
        strategy = _wire_strategy(manager)
        commit = AsyncMock(return_value=SettlementCommitOutcome("l", False, None))

        result = await manager.run_settlement_cycle(strategy, settlement_commit=commit)

        assert result.success is True
        assert result.accounting_degraded is False
        legs = [c.kwargs["leg"] for c in commit.await_args_list]
        # deposits-only path (auto_settle_redeems=False): propose + deposit.
        assert legs == ["propose", "deposit"]
        deposit_call = next(c for c in commit.await_args_list if c.kwargs["leg"] == "deposit")
        assert deposit_call.kwargs["vault_address"] == manager._config.vault_address
        assert deposit_call.kwargs["settlement_cycle_id"] == "settlement-3"  # epoch 2 + 1

    async def test_degraded_commit_never_blocks_state_machine(self):
        """A commit that reports degraded (or raises) must NOT halt settlement:
        the on-chain tx already landed. The cycle completes and surfaces the flag."""
        manager = _make_manager(vault_config=_make_config(auto_settle_redeems=False), vault_state=_idle_state())
        strategy = _wire_strategy(manager)
        commit = AsyncMock(return_value=SettlementCommitOutcome(None, True, "ledger: boom"))

        result = await manager.run_settlement_cycle(strategy, settlement_commit=commit)

        assert result.success is True  # state machine still progressed
        assert result.accounting_degraded is True

    async def test_commit_raising_never_blocks_state_machine(self):
        manager = _make_manager(vault_config=_make_config(auto_settle_redeems=False), vault_state=_idle_state())
        strategy = _wire_strategy(manager)
        commit = AsyncMock(side_effect=RuntimeError("commit boom"))

        result = await manager.run_settlement_cycle(strategy, settlement_commit=commit)

        assert result.success is True
        assert result.accounting_degraded is True

    async def test_no_commit_callable_is_noop(self):
        """Legacy / non-vault-runner path: no settlement_commit injected → zero
        commit code runs and the cycle behaves exactly as before."""
        manager = _make_manager(vault_config=_make_config(auto_settle_redeems=False), vault_state=_idle_state())
        strategy = _wire_strategy(manager)

        result = await manager.run_settlement_cycle(strategy)  # no settlement_commit

        assert result.success is True
        assert result.accounting_degraded is False
        assert manager._settlement_commit is None

    async def test_emit_settlement_commit_noop_when_unset(self):
        manager = _make_manager(vault_state=_idle_state())
        assert manager._settlement_commit is None
        # Must not raise and must not require any dependency.
        await manager._emit_settlement_commit(
            _make_strategy(), manager.get_vault_state(), leg="deposit", result=MagicMock()
        )

    def test_decimals_partial_failure_not_cached(self):
        """A transient resolver failure must NOT be pinned for the manager's lifetime.

        The manager is long-lived; caching a partial (None) resolution on the
        first cycle would leave every later settlement event unmeasured. Only a
        fully-resolved pair is cached; a partial result is returned for THIS
        cycle (Empty != Zero) and re-resolved next cycle.
        """
        manager = _make_manager(vault_state=_idle_state())
        assert manager._settlement_decimals is None
        strategy = _make_strategy()

        flaky = MagicMock()
        # First cycle: underlying resolves, share decimals read fails (RPC blip).
        flaky.get_decimals.side_effect = [6, RuntimeError("rpc blip")]
        with patch("almanak.framework.vault.lifecycle.get_token_resolver", return_value=flaky):
            assert manager._resolve_settlement_decimals(strategy) == (6, None)
        assert manager._settlement_decimals is None  # partial result NOT cached

        healthy = MagicMock()
        healthy.get_decimals.side_effect = [6, 18]
        with patch("almanak.framework.vault.lifecycle.get_token_resolver", return_value=healthy):
            assert manager._resolve_settlement_decimals(strategy) == (6, 18)
        assert manager._settlement_decimals == (6, 18)  # full pair cached

        # Cached: no further resolver calls.
        with patch("almanak.framework.vault.lifecycle.get_token_resolver") as never:
            assert manager._resolve_settlement_decimals(strategy) == (6, 18)
            never.assert_not_called()
