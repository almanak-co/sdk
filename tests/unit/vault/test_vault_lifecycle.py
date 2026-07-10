"""Tests for VaultLifecycleManager core: pre_decide_hook and state persistence."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.connectors.lagoon.receipt_parser import EVENT_TOPICS
from almanak.framework.vault.config import SettlementPhase, VaultAction, VaultConfig, VaultState
from almanak.framework.vault.lifecycle import VAULT_STATE_KEY, VaultLifecycleManager


def _uint256_hex(value: int) -> str:
    return f"{value:064x}"


def _indexed_topic(value: int) -> str:
    return "0x" + _uint256_hex(value)


def _encode_data(*values: int) -> str:
    return "0x" + "".join(_uint256_hex(value) for value in values)


def _settle_deposit_receipt() -> dict:
    return {
        "transactionHash": "0xdeposit123",
        "blockNumber": 12345,
        "status": 1,
        "logs": [
            {
                "address": "0xvault",
                "topics": [
                    EVENT_TOPICS["SettleDeposit"],
                    _indexed_topic(1),
                    _indexed_topic(0),
                ],
                "data": _encode_data(
                    1_000_000,
                    500_000,
                    200_000,
                    100_000,
                ),
            }
        ],
    }


def _make_config(**overrides) -> VaultConfig:
    defaults = {
        "vault_address": "0x1111111111111111111111111111111111111111",
        "valuator_address": "0x2222222222222222222222222222222222222222",
        "underlying_token": "USDC",
        "settlement_interval_minutes": 60,
    }
    defaults.update(overrides)
    return VaultConfig(**defaults)


def _make_manager(
    vault_config: VaultConfig | None = None,
    deployment_id: str = "test-strategy-1",
    initial_vault_state: dict | None = None,
    receipt_parser_protocol: str = "lagoon",
    receipt_parser=None,
) -> VaultLifecycleManager:
    """Create a VaultLifecycleManager with mocked dependencies."""
    config = vault_config or _make_config()
    sdk = MagicMock()
    adapter = MagicMock()
    orchestrator = MagicMock()

    manager = VaultLifecycleManager(
        vault_config=config,
        vault_sdk=sdk,
        vault_adapter=adapter,
        execution_orchestrator=orchestrator,
        deployment_id=deployment_id,
        initial_vault_state=initial_vault_state,
        receipt_parser_protocol=receipt_parser_protocol,
        receipt_parser=receipt_parser,
    )
    return manager


class TestPreDecideHookReturnsHold:
    """pre_decide_hook returns HOLD when settlement interval has not elapsed."""

    def test_hold_when_recently_settled(self):
        manager = _make_manager()
        # Set vault state with recent valuation
        manager._vault_state = VaultState(
            last_valuation_time=datetime.now(UTC) - timedelta(minutes=10),
            settlement_phase=SettlementPhase.IDLE,
            initialized=True,
        )

        result = manager.pre_decide_hook(strategy=MagicMock())
        assert result == VaultAction.HOLD

    def test_hold_when_just_under_interval(self):
        manager = _make_manager(vault_config=_make_config(settlement_interval_minutes=60))
        manager._vault_state = VaultState(
            last_valuation_time=datetime.now(UTC) - timedelta(minutes=59),
            settlement_phase=SettlementPhase.IDLE,
            initialized=True,
        )

        result = manager.pre_decide_hook(strategy=MagicMock())
        assert result == VaultAction.HOLD


class TestPreDecideHookReturnsSettle:
    """pre_decide_hook returns SETTLE when settlement interval has elapsed."""

    def test_settle_when_interval_elapsed(self):
        manager = _make_manager(vault_config=_make_config(settlement_interval_minutes=60))
        manager._vault_state = VaultState(
            last_valuation_time=datetime.now(UTC) - timedelta(minutes=61),
            settlement_phase=SettlementPhase.IDLE,
            initialized=True,
        )

        result = manager.pre_decide_hook(strategy=MagicMock())
        assert result == VaultAction.SETTLE

    def test_settle_when_never_settled(self):
        """First time: last_valuation_time is None -> should settle."""
        manager = _make_manager()
        manager._vault_state = VaultState(
            last_valuation_time=None,
            settlement_phase=SettlementPhase.IDLE,
            initialized=False,
        )

        result = manager.pre_decide_hook(strategy=MagicMock())
        assert result == VaultAction.SETTLE

    def test_settle_when_exactly_at_interval(self):
        manager = _make_manager(vault_config=_make_config(settlement_interval_minutes=60))
        manager._vault_state = VaultState(
            last_valuation_time=datetime.now(UTC) - timedelta(minutes=60),
            settlement_phase=SettlementPhase.IDLE,
            initialized=True,
        )

        result = manager.pre_decide_hook(strategy=MagicMock())
        assert result == VaultAction.SETTLE


class TestPreDecideHookReturnsResumeSettle:
    """pre_decide_hook returns RESUME_SETTLE when settlement_phase is not IDLE."""

    def test_resume_when_proposing(self):
        manager = _make_manager()
        manager._vault_state = VaultState(
            settlement_phase=SettlementPhase.PROPOSING,
            initialized=True,
        )

        result = manager.pre_decide_hook(strategy=MagicMock())
        assert result == VaultAction.RESUME_SETTLE

    def test_resume_when_proposed(self):
        manager = _make_manager()
        manager._vault_state = VaultState(
            settlement_phase=SettlementPhase.PROPOSED,
            initialized=True,
        )

        result = manager.pre_decide_hook(strategy=MagicMock())
        assert result == VaultAction.RESUME_SETTLE

    def test_resume_when_settling(self):
        manager = _make_manager()
        manager._vault_state = VaultState(
            settlement_phase=SettlementPhase.SETTLING,
            initialized=True,
        )

        result = manager.pre_decide_hook(strategy=MagicMock())
        assert result == VaultAction.RESUME_SETTLE

    def test_resume_when_settled(self):
        manager = _make_manager()
        manager._vault_state = VaultState(
            settlement_phase=SettlementPhase.SETTLED,
            initialized=True,
        )

        result = manager.pre_decide_hook(strategy=MagicMock())
        assert result == VaultAction.RESUME_SETTLE


class TestSettleDepositReceiptParsing:
    """Settle-deposit receipt parsing feeds vault accounting hints."""

    def test_parse_settle_deposit_receipt_uses_real_registry(self):
        manager = _make_manager(receipt_parser_protocol="lagoon")
        settle_result = SimpleNamespace(receipt=_settle_deposit_receipt())

        assert manager._parse_settle_deposit_receipt(settle_result) == (200_000, 100_000)

    def test_parse_settle_deposit_reads_per_tx_receipts(self):
        """ExecutionResult has NO top-level .receipt — per-tx receipts must be read.

        Real-fork finding (VIB-5666 proof run): the old top-level read meant the
        parser never ran on real ExecutionResults, so every settlement leg booked
        zero deltas despite real capital moving on-chain.
        """
        manager = _make_manager(receipt_parser_protocol="lagoon")
        # Realistic shape: receipts live per transaction; the settle tx is not
        # necessarily first (e.g. preceded by an eventless approve).
        eventless = {"transactionHash": "0xapprove", "status": 1, "logs": []}
        settle_result = SimpleNamespace(
            transaction_results=[
                SimpleNamespace(receipt=eventless),
                SimpleNamespace(receipt=_settle_deposit_receipt()),
            ],
        )

        assert manager._parse_settle_deposit_receipt(settle_result) == (200_000, 100_000)

    def test_parse_settle_deposit_dataclass_receipt_normalized(self):
        """TransactionReceipt-style objects are normalized via to_dict() for the parser."""
        manager = _make_manager(receipt_parser_protocol="lagoon")
        raw = _settle_deposit_receipt()

        class _ReceiptObj:
            def to_dict(self):
                return raw

        settle_result = SimpleNamespace(transaction_results=[SimpleNamespace(receipt=_ReceiptObj())])

        assert manager._parse_settle_deposit_receipt(settle_result) == (200_000, 100_000)

    def test_parse_settle_deposit_no_event_is_unmeasured(self, caplog):
        """Receipts parse fine but carry no SettleDeposit event -> (None, None), not 0."""
        manager = _make_manager(receipt_parser_protocol="lagoon")
        eventless = {"transactionHash": "0xother", "status": 1, "logs": []}
        settle_result = SimpleNamespace(transaction_results=[SimpleNamespace(receipt=eventless)])

        with caplog.at_level("WARNING"):
            assert manager._parse_settle_deposit_receipt(settle_result) == (None, None)
        assert "unmeasured" in caplog.text

    def test_parse_settle_deposit_no_receipts_is_unmeasured(self):
        manager = _make_manager(receipt_parser_protocol="lagoon")
        assert manager._parse_settle_deposit_receipt(SimpleNamespace()) == (None, None)

    def test_parse_settle_deposit_receipt_prefers_injected_parser(self):
        parser = MagicMock()
        parser.parse_receipt.return_value = SimpleNamespace(
            settle_deposits=[
                SimpleNamespace(
                    assets_deposited=333_000,
                    shares_minted=111_000,
                )
            ]
        )
        manager = _make_manager(
            receipt_parser_protocol="unknown-protocol",
            receipt_parser=parser,
        )
        settle_result = SimpleNamespace(receipt=_settle_deposit_receipt())

        assert manager._parse_settle_deposit_receipt(settle_result) == (333_000, 111_000)
        parser.parse_receipt.assert_called_once_with(settle_result.receipt)

    def test_parse_settle_deposit_receipt_warns_when_parser_cannot_be_resolved(self, caplog):
        manager = _make_manager(receipt_parser_protocol="unknown-protocol")
        settle_result = SimpleNamespace(receipt=_settle_deposit_receipt())

        with caplog.at_level("WARNING"):
            assert manager._parse_settle_deposit_receipt(settle_result) == (None, None)
        assert "Could not resolve receipt parser" in caplog.text

    def test_parse_settle_deposit_receipt_warns_on_malformed_settle_deposit(self, caplog):
        parser = MagicMock()
        parser.parse_receipt.return_value = SimpleNamespace(settle_deposits=[object()])
        manager = _make_manager(receipt_parser=parser)
        settle_result = SimpleNamespace(receipt=_settle_deposit_receipt())

        with caplog.at_level("WARNING"):
            assert manager._parse_settle_deposit_receipt(settle_result) == (None, None)
        assert "Could not extract settle_deposit accounting fields" in caplog.text


class TestVaultStatePersistence:
    """Test state serialization, deserialization, and round-trip."""

    def test_default_state_when_no_saved_state(self):
        """When no state exists, get_vault_state returns default VaultState."""
        manager = _make_manager()
        state = manager.get_vault_state()

        assert state.last_valuation_time is None
        assert state.last_total_assets == 0
        assert state.settlement_phase == SettlementPhase.IDLE
        assert state.initialized is False

    def test_loads_from_initial_vault_state(self):
        """Vault state is loaded from the initial_vault_state dict."""
        now = datetime(2026, 2, 15, 12, 0, 0, tzinfo=UTC)
        vault_dict = {
            "last_valuation_time": now.isoformat(),
            "last_total_assets": 1_000_000,
            "last_proposed_total_assets": 1_010_000,
            "last_pending_deposits": 50_000,
            "last_settlement_epoch": 5,
            "settlement_phase": "proposed",
            "initialized": True,
        }

        manager = _make_manager(initial_vault_state=vault_dict)
        state = manager.get_vault_state()

        assert state.last_valuation_time == now
        assert state.last_total_assets == 1_000_000
        assert state.last_proposed_total_assets == 1_010_000
        assert state.last_pending_deposits == 50_000
        assert state.last_settlement_epoch == 5
        assert state.settlement_phase == SettlementPhase.PROPOSED
        assert state.initialized is True

    def test_save_and_reload_round_trip(self):
        """Saving vault state and reloading via get_vault_state_dict gives same values."""
        manager = _make_manager()

        now = datetime(2026, 2, 15, 14, 30, 0, tzinfo=UTC)
        manager._vault_state = VaultState(
            last_valuation_time=now,
            last_total_assets=2_000_000,
            last_proposed_total_assets=2_020_000,
            last_pending_deposits=100_000,
            last_settlement_epoch=10,
            settlement_phase=SettlementPhase.SETTLING,
            initialized=True,
        )

        # Save (in-memory)
        manager.save_vault_state()

        # Get the dict for external persistence
        vault_dict = manager.get_vault_state_dict()
        assert vault_dict is not None

        # Create a new manager with that dict as initial state
        manager2 = VaultLifecycleManager(
            vault_config=manager._config,
            vault_sdk=manager._vault_sdk,
            vault_adapter=manager._vault_adapter,
            execution_orchestrator=manager._execution_orchestrator,
            deployment_id="test-strategy-1",
            initial_vault_state=vault_dict,
        )

        state = manager2.get_vault_state()
        assert state.last_valuation_time == now
        assert state.last_total_assets == 2_000_000
        assert state.last_proposed_total_assets == 2_020_000
        assert state.last_pending_deposits == 100_000
        assert state.last_settlement_epoch == 10
        assert state.settlement_phase == SettlementPhase.SETTLING
        assert state.initialized is True

    def test_serialize_deserialize_none_valuation_time(self):
        """None valuation time serializes and deserializes correctly."""
        state = VaultState(last_valuation_time=None)
        serialized = VaultLifecycleManager._serialize_vault_state(state)
        deserialized = VaultLifecycleManager._deserialize_vault_state(serialized)

        assert deserialized.last_valuation_time is None

    def test_serialize_deserialize_all_phases(self):
        """All settlement phases round-trip through serialization."""
        for phase in SettlementPhase:
            state = VaultState(settlement_phase=phase)
            serialized = VaultLifecycleManager._serialize_vault_state(state)
            deserialized = VaultLifecycleManager._deserialize_vault_state(serialized)
            assert deserialized.settlement_phase == phase

    def test_get_vault_state_dict_returns_serialized(self):
        """get_vault_state_dict returns the serialized vault state."""
        manager = _make_manager()
        manager._vault_state = VaultState(
            last_total_assets=500_000,
            initialized=True,
        )

        vault_dict = manager.get_vault_state_dict()
        assert vault_dict is not None
        assert vault_dict["last_total_assets"] == 500_000
        assert vault_dict["initialized"] is True

    def test_get_vault_state_dict_returns_none_when_no_state(self):
        """get_vault_state_dict returns None when no vault state exists."""
        manager = _make_manager()
        # Don't access get_vault_state() -- keep _vault_state as None
        assert manager.get_vault_state_dict() is None

    def test_get_vault_state_caches(self):
        """get_vault_state caches the result and doesn't re-read on subsequent calls."""
        manager = _make_manager()
        state1 = manager.get_vault_state()
        state2 = manager.get_vault_state()
        assert state1 is state2

    def test_load_handles_missing_vault_key(self):
        """If initial_vault_state is None, returns defaults."""
        manager = _make_manager(initial_vault_state=None)
        state = manager.get_vault_state()

        assert state.settlement_phase == SettlementPhase.IDLE
        assert state.initialized is False


class TestPreDecideHookSettlementGate:
    """VIB-5664 — the settlement/rebalance interleave gate.

    A strategy exposing ``vault_settlement_allowed()`` defers a FRESH settlement
    while it is mid-rebalance, but an in-flight settlement always resumes.
    """

    def _due_state(self) -> VaultState:
        return VaultState(
            last_valuation_time=datetime.now(UTC) - timedelta(minutes=61),
            settlement_phase=SettlementPhase.IDLE,
            initialized=True,
        )

    def test_gate_false_defers_fresh_settlement(self):
        manager = _make_manager(vault_config=_make_config(settlement_interval_minutes=60))
        manager._vault_state = self._due_state()
        strategy = SimpleNamespace(vault_settlement_allowed=lambda: False)
        assert manager.pre_decide_hook(strategy=strategy) == VaultAction.HOLD

    def test_gate_true_allows_fresh_settlement(self):
        manager = _make_manager(vault_config=_make_config(settlement_interval_minutes=60))
        manager._vault_state = self._due_state()
        strategy = SimpleNamespace(vault_settlement_allowed=lambda: True)
        assert manager.pre_decide_hook(strategy=strategy) == VaultAction.SETTLE

    def test_in_flight_settlement_resumes_even_if_gate_false(self):
        """An interrupted settlement must NEVER be gated (would strand a proposal)."""
        manager = _make_manager()
        manager._vault_state = VaultState(
            last_valuation_time=datetime.now(UTC),
            settlement_phase=SettlementPhase.PROPOSING,
            initialized=True,
        )
        strategy = SimpleNamespace(vault_settlement_allowed=lambda: False)
        assert manager.pre_decide_hook(strategy=strategy) == VaultAction.RESUME_SETTLE

    def test_raising_gate_defers_fail_safe(self):
        manager = _make_manager(vault_config=_make_config(settlement_interval_minutes=60))
        manager._vault_state = self._due_state()

        def _boom() -> bool:
            raise RuntimeError("gate bug")

        strategy = SimpleNamespace(vault_settlement_allowed=_boom)
        assert manager.pre_decide_hook(strategy=strategy) == VaultAction.HOLD

    def test_no_gate_method_settles_as_before(self):
        manager = _make_manager(vault_config=_make_config(settlement_interval_minutes=60))
        manager._vault_state = self._due_state()
        strategy = SimpleNamespace()  # no vault_settlement_allowed attribute
        assert manager.pre_decide_hook(strategy=strategy) == VaultAction.SETTLE
