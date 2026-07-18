"""Tests for VaultLifecycleManager.release_on_teardown (VIB-5667).

Vault-safe teardown: transition the vault Open->Closing->Closed so ALL
depositors — including a deposit-only user who never requested a redemption —
can redeem their capital. Covers the happy path, idempotent crash-resume from
each phase, the single-signer preflight guard, the NAV clamp / shortfall
degrade, and the release_on_teardown=False escape hatch.
"""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from almanak.framework.vault.config import (
    VaultConfig,
    VaultReleasePhase,
    VaultState,
)
from almanak.framework.vault.lifecycle import _MAX_UINT256, VaultLifecycleManager

# Single-signer alignment: owner == safe == valuator == wallet == config.valuator.
SIGNER = "0x3333333333333333333333333333333333333333"
VAULT = "0x1111111111111111111111111111111111111111"
UNDERLYING_TOKEN_ADDR = "0x4444444444444444444444444444444444444444"


def _make_config(**overrides) -> VaultConfig:
    defaults = {
        "vault_address": VAULT,
        "valuator_address": SIGNER,
        "underlying_token": "USDC",
        "settlement_interval_minutes": 60,
    }
    defaults.update(overrides)
    return VaultConfig(**defaults)


def _make_strategy(chain: str = "base", wallet_address: str = SIGNER):
    strategy = MagicMock()
    strategy.chain = chain
    strategy.wallet_address = wallet_address
    return strategy


def _make_market(underlying_balance: Decimal = Decimal("1000")):
    """MarketSnapshot whose ``balance(underlying)`` returns a Decimal balance."""
    market = MagicMock()
    bal = MagicMock()
    bal.balance = underlying_balance
    market.balance.return_value = bal
    return market


def _ok_result():
    r = MagicMock()
    r.success = True
    r.tx_hashes = ["0xabc"]
    r.receipts = [{}]
    return r


def _fail_result():
    r = MagicMock()
    r.success = False
    r.error = "revert"
    r.tx_hashes = []
    return r


def _make_manager(
    *,
    state: str = "Open",
    owner: str = SIGNER,
    safe: str = SIGNER,
    valuator: str = SIGNER,
    manager_shares: int = 0,
    obligations: int = 0,
    new_total_assets: int = 1_000_000_000,
    config_overrides: dict | None = None,
    vault_state: VaultState | None = None,
) -> VaultLifecycleManager:
    config = _make_config(**(config_overrides or {}))
    sdk = MagicMock()
    adapter = MagicMock()
    orchestrator = MagicMock()
    orchestrator.execute = AsyncMock(return_value=_ok_result())

    sdk.get_vault_state.return_value = state
    sdk.get_owner.return_value = owner
    sdk.get_roles_storage.return_value = {"safe": safe, "valuationManager": valuator}
    sdk.get_valuation_manager.return_value = valuator
    sdk.get_curator.return_value = safe
    sdk.get_total_assets.return_value = obligations
    sdk.get_underlying_balance.return_value = manager_shares
    sdk.get_underlying_token_address.return_value = UNDERLYING_TOKEN_ADDR
    sdk.build_approve_deposit_tx.return_value = {"to": UNDERLYING_TOKEN_ADDR, "from": safe, "data": "0x", "value": "0"}

    # Faithfully model Lagoon: ``updateNewTotalAssets`` sets the slot, and
    # ``newTotalAssets()`` reads back the LAST proposed value. So the release code,
    # after proposing the safety-approved NAV, reads back exactly that value and
    # closes with it. ``_slot`` seeds the value the vault holds BEFORE any release
    # propose (e.g. a resume into Closing where a prior run's proposal still sits in
    # the slot). Tests that need a DIVERGENT slot override
    # ``sdk.get_new_total_assets.side_effect`` after construction.
    _slot = {"nav": new_total_assets}

    def _capture_propose(params):
        _slot["nav"] = params.new_total_assets
        return MagicMock(name="propose_bundle")

    # Adapters return distinct sentinels so we can assert leg ordering.
    adapter.build_propose_valuation_bundle.side_effect = _capture_propose
    adapter.build_initiate_closing_bundle.return_value = MagicMock(name="initiate_bundle")
    adapter.build_close_bundle.return_value = MagicMock(name="close_bundle")
    adapter.build_redeem_bundle.return_value = MagicMock(name="redeem_bundle")
    sdk.get_new_total_assets.side_effect = lambda _addr: _slot["nav"]

    manager = VaultLifecycleManager(
        vault_config=config,
        vault_sdk=sdk,
        vault_adapter=adapter,
        execution_orchestrator=orchestrator,
        deployment_id="test-release",
    )
    if vault_state is not None:
        manager._vault_state = vault_state
    return manager


def _run(manager, strategy, market, commit=None):
    with patch("almanak.framework.vault.lifecycle.get_token_resolver") as mock_resolver:
        mock_resolver.return_value.get_decimals.return_value = 6
        return asyncio.run(manager.release_on_teardown(strategy, market, commit=commit))


class TestHappyPath:
    def test_open_to_closed_full_sequence(self):
        """Open vault: propose -> initiateClosing -> close -> redeem (in order)."""
        manager = _make_manager(state="Open", manager_shares=500, obligations=1_000_000_000)
        strategy = _make_strategy()
        market = _make_market(Decimal("1000"))  # 1000 USDC -> 1_000_000_000 raw

        result = _run(manager, strategy, market)

        assert result.released is True
        assert result.degraded is False
        assert result.manager_shares_redeemed == 500
        adapter = manager._vault_adapter
        adapter.build_propose_valuation_bundle.assert_called_once()
        adapter.build_initiate_closing_bundle.assert_called_once()
        adapter.build_close_bundle.assert_called_once()
        adapter.build_redeem_bundle.assert_called_once()
        # close() uses the SAFETY-APPROVED NAV (obligations = 1_000_000_000), which
        # the slot holds after the propose — NOT an arbitrary slot readback (audit #4).
        close_params = adapter.build_close_bundle.call_args.args[0]
        assert close_params.new_total_assets == 1_000_000_000
        assert manager.get_vault_state().release_phase == VaultReleasePhase.DEPOSITORS_RELEASED

    def test_close_uses_approved_nav_not_divergent_slot(self):
        """Audit #4: if the on-chain slot DIVERGES from the safety-approved NAV, the
        release re-proposes the approved value and closes with it — never the raw
        slot readback (which would bypass the obligations cap)."""
        # obligations 600 USDC; Safe holds 1000 → approved NAV = 600_000_000.
        manager = _make_manager(state="Open", obligations=600_000_000)
        # Slot lies: it reads a much larger value (e.g. a stale/foreign proposal) on
        # the FIRST read, then faithfully follows the re-propose.
        real = {"nav": None}
        calls = {"n": 0}

        def _propose(params):
            real["nav"] = params.new_total_assets
            return MagicMock(name="propose_bundle")

        def _read(_addr):
            calls["n"] += 1
            # First read (right after initiateClosing) diverges; later reads reflect
            # whatever was last proposed (the approved value).
            return 999_000_000_000 if calls["n"] == 1 else real["nav"]

        manager._vault_adapter.build_propose_valuation_bundle.side_effect = _propose
        manager._vault_sdk.get_new_total_assets.side_effect = _read

        result = _run(manager, _make_strategy(), _make_market(Decimal("1000")))
        assert result.released is True
        close_params = manager._vault_adapter.build_close_bundle.call_args.args[0]
        assert close_params.new_total_assets == 600_000_000  # approved, NOT 999_000_000_000

    def test_close_degrades_when_slot_cannot_hold_approved(self):
        """Audit #4: if the slot cannot be made to hold the approved NAV (persistently
        divergent), refuse to close() with the unvetted value — degrade instead."""
        manager = _make_manager(state="Open", obligations=600_000_000)
        # Slot ALWAYS reads a foreign value regardless of what we propose.
        manager._vault_sdk.get_new_total_assets.side_effect = lambda _addr: 999_000_000_000
        result = _run(manager, _make_strategy(), _make_market(Decimal("1000")))
        assert result.degraded is True
        assert result.released is False
        manager._vault_adapter.build_close_bundle.assert_not_called()


class TestIdempotentResume:
    def test_already_closed_only_redeems(self):
        """Closed vault: skip propose/initiate/close, redeem manager shares only."""
        manager = _make_manager(state="Closed", manager_shares=42)
        result = _run(manager, _make_strategy(), _make_market())
        assert result.released is True
        assert result.manager_shares_redeemed == 42
        adapter = manager._vault_adapter
        adapter.build_propose_valuation_bundle.assert_not_called()
        adapter.build_initiate_closing_bundle.assert_not_called()
        adapter.build_close_bundle.assert_not_called()
        adapter.build_redeem_bundle.assert_called_once()

    def test_already_closed_no_shares_is_noop_success(self):
        manager = _make_manager(state="Closed", manager_shares=0)
        result = _run(manager, _make_strategy(), _make_market())
        assert result.released is True
        assert result.manager_shares_redeemed == 0
        manager._vault_adapter.build_redeem_bundle.assert_not_called()

    def test_resume_from_closing_skips_propose_and_initiate(self):
        """Closing vault resumed with the persisted approved NAV already in the slot:
        close with the approved value, no re-propose/initiate."""
        # A prior run reached CLOSING_INITIATED with an approved NAV of 555; the slot
        # still holds 555 (seeded via new_total_assets).
        vs = VaultState(release_phase=VaultReleasePhase.CLOSING_INITIATED, release_final_nav=555)
        manager = _make_manager(state="Closing", new_total_assets=555, manager_shares=0, vault_state=vs)
        result = _run(manager, _make_strategy(), _make_market())
        assert result.released is True
        adapter = manager._vault_adapter
        adapter.build_initiate_closing_bundle.assert_not_called()
        adapter.build_propose_valuation_bundle.assert_not_called()  # slot already holds approved
        adapter.build_close_bundle.assert_called_once()
        assert adapter.build_close_bundle.call_args.args[0].new_total_assets == 555

    def test_resume_from_closing_reproposes_when_slot_consumed(self):
        """Closing resumed but the slot was consumed (max sentinel) -> re-propose the
        PERSISTED approved NAV, then close with it (not a raw readback)."""
        vs = VaultState(release_phase=VaultReleasePhase.CLOSING_INITIATED, release_final_nav=900_000)
        manager = _make_manager(state="Closing", manager_shares=0, vault_state=vs)
        # First read is the consumed sentinel; after re-proposing 900_000 the slot
        # faithfully reflects it.
        real = {"nav": _MAX_UINT256}
        calls = {"n": 0}

        def _propose(params):
            real["nav"] = params.new_total_assets
            return MagicMock(name="propose_bundle")

        def _read(_addr):
            calls["n"] += 1
            return _MAX_UINT256 if calls["n"] == 1 else real["nav"]

        manager._vault_adapter.build_propose_valuation_bundle.side_effect = _propose
        manager._vault_sdk.get_new_total_assets.side_effect = _read
        result = _run(manager, _make_strategy(), _make_market(Decimal("1")))
        assert result.released is True
        manager._vault_adapter.build_propose_valuation_bundle.assert_called_once()
        assert manager._vault_adapter.build_close_bundle.call_args.args[0].new_total_assets == 900_000


class TestSingleSignerGuard:
    def test_owner_mismatch_degrades_not_closes(self):
        manager = _make_manager(state="Open", owner="0x9999999999999999999999999999999999999999")
        result = _run(manager, _make_strategy(), _make_market())
        assert result.degraded is True
        assert result.released is False
        assert "single-signer" in result.reason
        manager._vault_adapter.build_close_bundle.assert_not_called()

    def test_valuator_mismatch_degrades(self):
        manager = _make_manager(state="Open", valuator="0x8888888888888888888888888888888888888888")
        result = _run(manager, _make_strategy(), _make_market())
        assert result.degraded is True
        manager._vault_adapter.build_initiate_closing_bundle.assert_not_called()

    def test_safe_mismatch_degrades(self):
        manager = _make_manager(state="Open", safe="0x7777777777777777777777777777777777777777")
        result = _run(manager, _make_strategy(), _make_market())
        assert result.degraded is True


class TestNavClampAndShortfall:
    def test_clamps_nav_to_realized_within_tolerance(self):
        """Realized slightly below obligations (within tolerance) -> clamp + close."""
        # obligations 1_000_000_000; realized 990 USDC = 990_000_000 (1% short -> within 5%).
        manager = _make_manager(state="Open", obligations=1_000_000_000, manager_shares=0)
        result = _run(manager, _make_strategy(), _make_market(Decimal("990")))
        assert result.released is True
        propose_params = manager._vault_adapter.build_propose_valuation_bundle.call_args.args[0]
        assert propose_params.new_total_assets == 990_000_000

    def test_backs_obligations_not_full_safe_balance(self):
        """Safe holds MORE than obligations (e.g. manager seed capital) -> propose
        obligations, not the full Safe balance (don't pay depositors the excess)."""
        # obligations 600_000_000; Safe holds 1000 USDC = 1_000_000_000 realized.
        manager = _make_manager(state="Open", obligations=600_000_000, manager_shares=0)
        result = _run(manager, _make_strategy(), _make_market(Decimal("1000")))
        assert result.released is True
        propose_params = manager._vault_adapter.build_propose_valuation_bundle.call_args.args[0]
        assert propose_params.new_total_assets == 600_000_000

    def test_genuine_shortfall_degrades_does_not_force_close(self):
        """Realized far below obligations -> degrade, do NOT force a haircut close."""
        # obligations 1_000_000_000; realized 500 USDC = 500_000_000 (50% short > 5%).
        manager = _make_manager(state="Open", obligations=1_000_000_000, manager_shares=0)
        result = _run(manager, _make_strategy(), _make_market(Decimal("500")))
        assert result.degraded is True
        assert result.released is False
        assert "shortfall" in result.reason.lower()
        manager._vault_adapter.build_close_bundle.assert_not_called()

    def test_reads_fresh_post_unwind_balance(self):
        """Audit #2: the teardown snapshot predates the LP unwind, so its balance
        cache is stale. Release must evict the memo before the NAV read so it
        reflects the realized POST-unwind Safe balance."""
        manager = _make_manager(state="Open", obligations=600_000_000)
        market = _make_market(Decimal("1000"))
        _run(manager, _make_strategy(), market)
        market.invalidate_balance.assert_called_once_with(manager._config.underlying_token)

    def test_unmeasured_safe_balance_degrades(self):
        """Empty != Zero: an unread Safe balance must not propose an unbacked NAV."""
        manager = _make_manager(state="Open")
        market = MagicMock()
        market.balance.side_effect = RuntimeError("no balance provider")
        result = _run(manager, _make_strategy(), market)
        assert result.degraded is True
        manager._vault_adapter.build_propose_valuation_bundle.assert_not_called()


class TestConfigAndFailures:
    def test_release_disabled_skips_entirely(self):
        manager = _make_manager(state="Open", config_overrides={"release_on_teardown": False})
        result = _run(manager, _make_strategy(), _make_market())
        assert result.skipped is True
        assert result.released is False
        manager._vault_sdk.get_vault_state.assert_not_called()

    def test_close_failure_degrades(self):
        manager = _make_manager(state="Open")

        async def _exec(bundle, wallet_address=None):
            # Fail only the close bundle; everything else succeeds.
            if bundle is manager._vault_adapter.build_close_bundle.return_value:
                return _fail_result()
            return _ok_result()

        manager._execution_orchestrator.execute = AsyncMock(side_effect=_exec)
        result = _run(manager, _make_strategy(), _make_market(Decimal("1000")))
        assert result.degraded is True
        assert result.released is False

    def test_unreadable_state_degrades(self):
        manager = _make_manager(state="Open")
        manager._vault_sdk.get_vault_state.side_effect = RuntimeError("rpc down")
        result = _run(manager, _make_strategy(), _make_market())
        assert result.degraded is True

    def test_version_mismatch_degrades_before_reading_state(self):
        """Audit #7: verify the on-chain impl version BEFORE decoding the v0.5.0
        storage slot. A mismatch degrades loudly and never reads/mutates state."""
        manager = _make_manager(state="Open")
        manager._vault_sdk.verify_version.side_effect = RuntimeError("expected v0.5.0, got v0.4.0")
        result = _run(manager, _make_strategy(), _make_market())
        assert result.degraded is True
        assert "version" in result.reason.lower()
        manager._vault_sdk.get_vault_state.assert_not_called()

    def test_empty_vault_proposes_zero_not_full_safe(self):
        """Audit #6: with no settled obligations (totalAssets<=0), propose NAV 0 —
        never the full realized Safe balance (which would pull non-depositor capital
        into a Closed vault nobody can redeem)."""
        manager = _make_manager(state="Open", obligations=0)
        result = _run(manager, _make_strategy(), _make_market(Decimal("1000")))
        propose_params = manager._vault_adapter.build_propose_valuation_bundle.call_args.args[0]
        assert propose_params.new_total_assets == 0
        assert result.released is True
        # And crucially close() uses the approved 0, NOT the Safe's realized balance
        # (audit #4 — otherwise non-depositor capital lands in a Closed empty vault).
        close_params = manager._vault_adapter.build_close_bundle.call_args.args[0]
        assert close_params.new_total_assets == 0


class TestCommitPairing:
    def test_every_successful_execute_is_committed(self):
        """The anti-bypass invariant: each release leg drives the commit pipeline."""
        manager = _make_manager(state="Open", manager_shares=100, obligations=1_000_000_000)
        commit = AsyncMock()
        result = _run(manager, _make_strategy(), _make_market(Decimal("1000")), commit=commit)
        assert result.released is True
        # propose + initiate + approve + close + redeem = 5 successful legs, 5 commits.
        assert commit.await_count == manager._execution_orchestrator.execute.await_count
        assert commit.await_count >= 5
        action_types = {c.kwargs["action_type"] for c in commit.await_args_list}
        assert {"PROPOSE_VAULT_VALUATION", "INITIATE_VAULT_CLOSING", "CLOSE_VAULT", "REDEEM_VAULT"} <= action_types

    def test_failed_execute_is_not_committed(self):
        manager = _make_manager(state="Open")
        manager._execution_orchestrator.execute = AsyncMock(return_value=_fail_result())
        commit = AsyncMock()
        _run(manager, _make_strategy(), _make_market(Decimal("1000")), commit=commit)
        commit.assert_not_awaited()
