"""AsterPerpsAdapter / AsterPerpsConfig validation (VIB-3045).

Locks the "broker_id is required" invariant that replaced the old
``PancakeSwapPerpsConfig.broker_id = 2`` default.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from almanak.framework.connectors.aster_perps import (
    ASTER_BROKER_RAW,
    PCS_BROKER_ID,
    AsterPerpsAdapter,
    AsterPerpsConfig,
)


class TestBrokerIdRequired:
    def test_construct_with_raw_broker_ok(self) -> None:
        adapter = AsterPerpsAdapter(AsterPerpsConfig(broker_id=ASTER_BROKER_RAW))
        assert adapter.config.broker_id == 0
        assert adapter.config.chain == "bsc"
        assert adapter.router  # resolved from registry

    def test_construct_with_pcs_broker_ok(self) -> None:
        adapter = AsterPerpsAdapter(AsterPerpsConfig(broker_id=PCS_BROKER_ID))
        assert adapter.config.broker_id == 2

    def test_rejects_none_broker_id(self) -> None:
        """Belt-and-braces: even if a caller sneaks None past the dataclass, adapter rejects."""
        cfg = replace(AsterPerpsConfig(broker_id=0), broker_id=None)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="broker_id is required"):
            AsterPerpsAdapter(cfg)

    def test_missing_broker_id_positional_fails(self) -> None:
        """AsterPerpsConfig itself is a dataclass with no broker_id default."""
        with pytest.raises(TypeError):
            AsterPerpsConfig()  # type: ignore[call-arg] — missing required arg


class TestChainValidation:
    def test_bsc_accepted(self) -> None:
        AsterPerpsAdapter(AsterPerpsConfig(broker_id=0, chain="bsc"))  # no raise

    def test_arbitrum_rejected_phase_1(self) -> None:
        """Phase 1 is BSC-only; DR-V2ABI / DR-CHAINS gate multi-chain expansion."""
        with pytest.raises(ValueError, match="chain='bsc' only"):
            AsterPerpsAdapter(AsterPerpsConfig(broker_id=0, chain="arbitrum"))

    def test_ethereum_rejected_phase_1(self) -> None:
        with pytest.raises(ValueError, match="chain='bsc' only"):
            AsterPerpsAdapter(AsterPerpsConfig(broker_id=0, chain="ethereum"))


class TestBuildOpenSurface:
    """Fine-grained adapter.build_open() validation (unit, no on-chain calls)."""

    @pytest.fixture
    def adapter(self) -> AsterPerpsAdapter:
        return AsterPerpsAdapter(AsterPerpsConfig(broker_id=PCS_BROKER_ID))

    def test_unknown_market_returns_failure(self, adapter: AsterPerpsAdapter) -> None:
        result = adapter.build_open(
            market="DOGE/USD",  # not in ASTER_PERPS_MARKETS['bsc']
            collateral_token="USDT",
            collateral_amount=Decimal("500"),
            collateral_decimals=18,
            size_usd=Decimal("250"),
            mark_price=Decimal("0.08"),
            is_long=True,
            max_slippage=Decimal("0.01"),
        )
        assert not result.success
        assert "not registered for Aster Perps" in result.error

    def test_zero_collateral_rejected(self, adapter: AsterPerpsAdapter) -> None:
        result = adapter.build_open(
            market="BTC/USD",
            collateral_token="USDT",
            collateral_amount=Decimal("0"),
            collateral_decimals=18,
            size_usd=Decimal("250"),
            mark_price=Decimal("95000"),
            is_long=True,
            max_slippage=Decimal("0.01"),
        )
        assert not result.success
        assert "must be positive" in result.error

    def test_native_bnb_routes_via_value_carrying_tx(self, adapter: AsterPerpsAdapter) -> None:
        """BNB margin must produce a value-carrying tx (openMarketTradeBNB)."""
        result = adapter.build_open(
            market="BTC/USD",
            collateral_token="BNB",
            collateral_amount=Decimal("0.3"),
            collateral_decimals=18,
            size_usd=Decimal("500"),
            mark_price=Decimal("95000"),
            is_long=True,
            max_slippage=Decimal("0.01"),
        )
        assert result.success, result.error
        assert result.native is True
        assert result.tx is not None
        assert result.tx.value == result.amount_in_wei > 0

    def test_erc20_margin_routes_via_non_value_tx(self, adapter: AsterPerpsAdapter) -> None:
        """USDT margin must NOT be sent via msg.value (compiler prepends approve)."""
        result = adapter.build_open(
            market="BTC/USD",
            collateral_token="USDT",
            collateral_amount=Decimal("500"),
            collateral_decimals=18,
            size_usd=Decimal("500"),
            mark_price=Decimal("95000"),
            is_long=True,
            max_slippage=Decimal("0.01"),
        )
        assert result.success, result.error
        assert result.native is False
        assert result.tx is not None
        assert result.tx.value == 0


class TestBuildClose:
    def test_close_emits_bytes32_calldata(self) -> None:
        adapter = AsterPerpsAdapter(AsterPerpsConfig(broker_id=PCS_BROKER_ID))
        trade_hash = "0x" + "ab" * 32  # arbitrary 32-byte hex
        tx = adapter.build_close(trade_hash=trade_hash)
        # 4-byte selector + 32-byte payload = 36 bytes
        assert len(tx.data) == 36
        assert tx.value == 0
        assert tx.to == adapter.router
