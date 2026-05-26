"""Regression guards for VIB-3825 (QA-PostFixes April31 BUG-35).

``aave_v3_aerodrome_leveraged_lp_base`` (Aave V3 + Aerodrome leveraged LP on
Base) failed in the April-31 harness with a BORROW revert. The likely class of
cause was a per-reserve ``borrowingEnabled=false`` flag (Aave V3 short-string
code ``11`` — BORROWING_NOT_ENABLED) — burning gas + retry iterations every
loop tick.

VIB-3825 is the framework-level "fail before on-chain" mirror of
VIB-3744 / VIB-3815 / VIB-3816 / VIB-3818 / VIB-3823:

1. ``_DecodedReserveConfig`` now carries the ``borrowing_enabled`` field
   (decoded from word 6 of ``getReserveConfigurationData``).
2. ``_check_lending_reserve_borrowable`` returns a human reason when the
   reserve has ``borrowingEnabled=False``; ``compile_borrow`` translates this
   to a typed :class:`LendingBorrowNotEnabledError`.
3. ``IntentStateMachine._categorize_error`` lists the ERROR_PREFIX in
   ``permanent_keywords`` so retrying with the same asset never enters the
   retry-storm.
4. The Aave-compatible borrow path also calls ``_check_lending_reserve_active``
   on the borrow asset *before* the borrowable check, so a paused / frozen
   reserve fails the compile via the same ``CompilationStatus.FAILED`` path as
   the SUPPLY pre-flight (VIB-3744). The two governance bits are independent
   on Aave V3 — a frozen reserve can still report ``borrowingEnabled=true``
   and would revert on-chain — so both checks must run.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.base.lending.aave_helpers import (
    _check_lending_reserve_borrowable,
    _DecodedReserveConfig,
)
from almanak.framework.intents import BorrowIntent
from almanak.framework.intents.intent_errors import LendingBorrowNotEnabledError
from almanak.framework.intents.state_machine import IntentStateMachine


def _make_compiler(chain: str = "base") -> MagicMock:
    """Minimal compiler stub for the borrowable check."""
    compiler = MagicMock()
    compiler.chain = chain
    compiler.rpc_timeout = 5.0
    compiler._gateway_client = MagicMock()
    compiler._gateway_client.is_connected = True
    # Return a real dict so the helper's isinstance(cache, dict) check succeeds.
    compiler._lending_borrowable_cache = {}
    return compiler


class TestDecodedReserveConfigBorrowingField:
    def test_borrowing_enabled_round_trip(self) -> None:
        cfg = _DecodedReserveConfig(
            ltv=8000,
            usage_as_collateral_enabled=True,
            borrowing_enabled=True,
            is_active=True,
            is_frozen=False,
        )
        assert cfg.borrowing_enabled is True

    def test_borrowing_disabled_round_trip(self) -> None:
        cfg = _DecodedReserveConfig(
            ltv=0,
            usage_as_collateral_enabled=False,
            borrowing_enabled=False,
            is_active=True,
            is_frozen=False,
        )
        assert cfg.borrowing_enabled is False


class TestCheckLendingReserveBorrowable:
    def test_borrowing_enabled_returns_none(self) -> None:
        compiler = _make_compiler()
        with patch(
            "almanak.connectors._strategy_base.base.lending.aave_helpers._fetch_reserve_config"
        ) as mock_fetch:
            mock_fetch.return_value = _DecodedReserveConfig(
                ltv=8000,
                usage_as_collateral_enabled=True,
                borrowing_enabled=True,
                is_active=True,
                is_frozen=False,
            )
            result = _check_lending_reserve_borrowable(
                compiler,
                "0xa0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "USDC",
                "aave_v3",
            )
        assert result is None

    def test_borrowing_disabled_returns_reason(self) -> None:
        compiler = _make_compiler()
        with patch(
            "almanak.connectors._strategy_base.base.lending.aave_helpers._fetch_reserve_config"
        ) as mock_fetch:
            mock_fetch.return_value = _DecodedReserveConfig(
                ltv=8000,
                usage_as_collateral_enabled=True,
                borrowing_enabled=False,
                is_active=True,
                is_frozen=False,
            )
            result = _check_lending_reserve_borrowable(
                compiler,
                "0xa0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "USDC",
                "aave_v3",
            )
        assert result is not None
        assert "USDC" in result
        assert "borrowingEnabled=False" in result
        assert "aave_v3" in result

    def test_fetch_returns_none_yields_fail_open(self) -> None:
        compiler = _make_compiler()
        with patch(
            "almanak.connectors._strategy_base.base.lending.aave_helpers._fetch_reserve_config",
            return_value=None,
        ):
            result = _check_lending_reserve_borrowable(
                compiler,
                "0xa0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "USDC",
                "aave_v3",
            )
        assert result is None

    def test_unknown_chain_protocol_skips(self) -> None:
        """No PoolDataProvider registration → fail-open, no fetch attempted."""
        compiler = _make_compiler(chain="solana")
        with patch(
            "almanak.connectors._strategy_base.base.lending.aave_helpers._fetch_reserve_config"
        ) as mock_fetch:
            result = _check_lending_reserve_borrowable(
                compiler,
                "0x0000000000000000000000000000000000000000",
                "USDC",
                "aave_v3",
            )
        assert result is None
        mock_fetch.assert_not_called()


class TestLendingBorrowNotEnabledErrorMessage:
    def test_includes_chain_protocol_asset(self) -> None:
        err = LendingBorrowNotEnabledError(
            chain="base",
            protocol="aave_v3",
            asset_symbol="USDC",
            asset_address="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            reason="Reserve USDC on aave_v3 base is not borrowable (borrowingEnabled=False).",
        )
        msg = str(err)
        assert err.ERROR_PREFIX in msg
        assert "USDC" in msg
        assert "aave_v3" in msg
        assert "base" in msg


class TestStateMachineClassifiesBorrowNotEnabledAsPermanent:
    @pytest.fixture()
    def sm(self) -> IntentStateMachine:
        return IntentStateMachine.__new__(IntentStateMachine)

    def test_typed_error_prefix_is_permanent(self, sm: IntentStateMachine) -> None:
        msg = (
            "Lending borrow not enabled for USDC on aave_v3 base: "
            "Reserve USDC on aave_v3 base is not borrowable "
            "(borrowingEnabled=False)."
        )
        assert sm._categorize_error(msg) == "COMPILATION_PERMANENT"

    def test_random_borrow_revert_is_not_permanent(
        self, sm: IntentStateMachine
    ) -> None:
        # Sanity: don't over-classify generic borrow errors.
        msg = "Borrow failed: insufficient collateral"
        assert sm._categorize_error(msg) != "COMPILATION_PERMANENT"


class TestBorrowFrozenReserveGate:
    """Pin the new ``_check_lending_reserve_active`` gate on the BORROW path.

    The borrowable check must run *after* the active/frozen check so a paused
    reserve fails the compile via ``CompilationStatus.FAILED`` (cheaper, hits
    the cache shared with SUPPLY) rather than via the typed
    ``LendingBorrowNotEnabledError`` that the borrowable path raises.
    """

    def _setup(self):
        from almanak.connectors._strategy_base.base.lending import aave_helpers as cl_mod
        from almanak.framework.intents.compiler_models import CompilationStatus

        compiler = MagicMock()
        compiler.chain = "base"
        compiler.wallet_address = "0x" + "1" * 40
        compiler._gateway_client = MagicMock()
        compiler._format_amount.side_effect = lambda a, d: str(a)
        compiler._build_approve_tx.return_value = []

        collateral = MagicMock(
            symbol="WETH", address="0x" + "ab" * 20, decimals=18, is_native=False,
        )
        borrow = MagicMock(
            symbol="USDC", address="0x" + "cd" * 20, decimals=6, is_native=False,
        )
        intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("500"),
        )
        return cl_mod, CompilationStatus, compiler, collateral, borrow, intent

    def test_frozen_reserve_returns_failed_before_borrowable_check(self) -> None:
        cl_mod, CompilationStatus, compiler, collateral, borrow, intent = self._setup()
        with patch.object(cl_mod, "_check_lending_reserve_active") as frozen_mock, \
             patch.object(cl_mod, "_check_lending_reserve_borrowable") as borrowable_mock, \
             patch("almanak.framework.intents.compiler_adapters.AaveV3Adapter") as adapter_cls:
            adapter_cls.return_value.get_pool_address.return_value = "0x" + "ee" * 20
            frozen_mock.return_value = (
                "Reserve USDC on aave_v3 base is not active "
                "(isActive=True, isFrozen=True). HOLD until governance reactivates."
            )
            result = cl_mod._compile_borrow_aave_compatible(
                compiler, intent, collateral, borrow, Decimal("1"),
            )
        assert result.status == CompilationStatus.FAILED
        assert "isFrozen=True" in result.error
        # The borrowable check must NOT have run — the frozen gate short-circuits.
        borrowable_mock.assert_not_called()

    def test_active_reserve_proceeds_to_borrowable_check(self) -> None:
        cl_mod, _CS, compiler, collateral, borrow, intent = self._setup()
        with patch.object(cl_mod, "_check_lending_reserve_active") as frozen_mock, \
             patch.object(cl_mod, "_check_lending_reserve_borrowable") as borrowable_mock, \
             patch("almanak.framework.intents.compiler_adapters.AaveV3Adapter") as adapter_cls:
            adapter_cls.return_value.get_pool_address.return_value = "0x" + "ee" * 20
            frozen_mock.return_value = None
            borrowable_mock.return_value = (
                "Reserve USDC on aave_v3 base is not borrowable (borrowingEnabled=False)."
            )
            with pytest.raises(LendingBorrowNotEnabledError):
                cl_mod._compile_borrow_aave_compatible(
                    compiler, intent, collateral, borrow, Decimal("1"),
                )
        # Both checks must have run when the reserve is active but not borrowable.
        frozen_mock.assert_called_once()
        borrowable_mock.assert_called_once()
