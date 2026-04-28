"""Tests for LP, Perp, and Vault category handlers (VIB-3470/3471/3472).

Regression tests prove drain_one actually writes events (not no-ops) for all
three categories after VIB-3478 removed the legacy _try_write_* methods.

Unit tests cover the key input/output contracts for each handler.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers.lp_handler import handle_lp
from almanak.framework.accounting.category_handlers.perp_handler import handle_perp
from almanak.framework.accounting.category_handlers.vault_handler import handle_vault
from almanak.framework.accounting.lp_accounting import LPAccountingEvent
from almanak.framework.accounting.models import LPEventType, PerpEventType, VaultEventType
from almanak.framework.accounting.perp_accounting import PerpAccountingEvent
from almanak.framework.accounting.processor import AccountingProcessor
from almanak.framework.accounting.vault_accounting import VaultAccountingEvent


# ──────────────────────────────────────────────────────────────────────────────
# Common builder helpers (mirror the style from test_accounting_processor.py)
# ──────────────────────────────────────────────────────────────────────────────


def _make_outbox_row(
    ledger_entry_id: str,
    intent_type: str,
    wallet_address: str = "0xwallet",
    position_key: str = "",
    market_id: str = "",
    status: str = "pending",
    attempts: int = 0,
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "ledger_entry_id": ledger_entry_id,
        "deployment_id": "dep-1",
        "strategy_id": "strat-1",
        "cycle_id": "cycle-1",
        "intent_type": intent_type,
        "wallet_address": wallet_address,
        "position_key": position_key,
        "market_id": market_id,
        "status": status,
        "attempts": attempts,
        "error": "",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _make_ledger_row(
    ledger_entry_id: str,
    intent_type: str,
    protocol: str = "aerodrome",
    chain: str = "base",
    token_in: str = "USDC",
    token_out: str = "DAI",
    amount_in: str = "100.0",
    amount_out: str = "100.0",
    tx_hash: str = "0xdeadbeef",
    extracted_data_json: str = "",
    price_inputs_json: str = "",
) -> dict[str, Any]:
    return {
        "id": ledger_entry_id,
        "strategy_id": "strat-1",
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": datetime.now(UTC).isoformat(),
        "intent_type": intent_type,
        "token_in": token_in,
        "amount_in": amount_in,
        "token_out": token_out,
        "amount_out": amount_out,
        "effective_price": "",
        "slippage_bps": None,
        "gas_used": 0,
        "gas_usd": "0.01",
        "tx_hash": tx_hash,
        "chain": chain,
        "protocol": protocol,
        "success": True,
        "error": "",
        "extracted_data_json": extracted_data_json,
        "price_inputs_json": price_inputs_json,
        "pre_state_json": "",
        "post_state_json": "",
    }


def _make_mock_store(
    outbox_row: dict | None = None,
    ledger_row: dict | None = None,
    already_written: bool = False,
) -> MagicMock:
    store = MagicMock()
    store.get_outbox_by_ledger_id = MagicMock(return_value=outbox_row)
    store.get_outbox_pending = MagicMock(return_value=[outbox_row] if outbox_row else [])
    store.update_outbox_entry = MagicMock()
    store.has_accounting_events_for_ledger = MagicMock(return_value=already_written)
    store.get_ledger_entry_by_id = MagicMock(return_value=ledger_row)
    store.save_accounting_event = AsyncMock(return_value=True)
    return store


# ──────────────────────────────────────────────────────────────────────────────
# Regression: drain_one must write events (not no-ops) for LP/Perp/Vault
# ──────────────────────────────────────────────────────────────────────────────


class TestDrainOneWritesLPEvent:
    @pytest.mark.asyncio
    async def test_drain_one_writes_lp_open_event(self) -> None:
        """drain_one on LP_OPEN outbox row produces an LPAccountingEvent, not None."""
        led_id = str(uuid.uuid4())
        position_key = "lp:aerodrome:base:0xwallet:0xpool"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            wallet_address="0xwallet",
            position_key=position_key,
            market_id="0xpool",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="USDC",
            token_out="DAI",
            amount_in="100.0",
            amount_out="100.0",
        )
        store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=False)
        proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

        result = await proc.drain_one(led_id)

        assert result is True, "drain_one must return True for LP_OPEN"
        # The regression: previously handler returned None → no write. Now it must write.
        store.save_accounting_event.assert_awaited_once()
        written_event = store.save_accounting_event.call_args[0][0]
        assert isinstance(written_event, LPAccountingEvent)
        assert written_event.event_type == LPEventType.LP_OPEN.value
        assert written_event.pool_address == "0xpool"

    @pytest.mark.asyncio
    async def test_drain_one_writes_lp_close_event(self) -> None:
        """drain_one on LP_CLOSE outbox row produces an LPAccountingEvent."""
        led_id = str(uuid.uuid4())
        position_key = "lp:uniswap_v3:arbitrum:0xwallet:0xpool2"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key=position_key,
            market_id="0xpool2",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
        )
        store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=False)
        proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

        result = await proc.drain_one(led_id)

        assert result is True
        store.save_accounting_event.assert_awaited_once()
        written_event = store.save_accounting_event.call_args[0][0]
        assert isinstance(written_event, LPAccountingEvent)
        assert written_event.event_type == LPEventType.LP_CLOSE.value


class TestDrainOneWritesPerpEvent:
    @pytest.mark.asyncio
    async def test_drain_one_writes_perp_open_event(self) -> None:
        """drain_one on PERP_OPEN outbox row produces a PerpAccountingEvent, not None."""
        led_id = str(uuid.uuid4())
        position_key = "perp:gmx_v2:arbitrum:0xwallet:eth/usd"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="PERP_OPEN",
            position_key=position_key,
            market_id="eth/usd",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="PERP_OPEN",
            protocol="gmx_v2",
            chain="arbitrum",
            token_in="USDC",
            token_out="",
            amount_in="500.0",
        )
        store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=False)
        proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

        result = await proc.drain_one(led_id)

        assert result is True, "drain_one must return True for PERP_OPEN"
        store.save_accounting_event.assert_awaited_once()
        written_event = store.save_accounting_event.call_args[0][0]
        assert isinstance(written_event, PerpAccountingEvent)
        assert written_event.event_type == PerpEventType.PERP_OPEN.value

    @pytest.mark.asyncio
    async def test_drain_one_writes_perp_close_event(self) -> None:
        """drain_one on PERP_CLOSE outbox row produces a PerpAccountingEvent."""
        led_id = str(uuid.uuid4())
        position_key = "perp:gmx_v2:arbitrum:0xwallet:eth/usd"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="PERP_CLOSE",
            position_key=position_key,
            market_id="eth/usd",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="PERP_CLOSE",
            protocol="gmx_v2",
            chain="arbitrum",
            token_in="USDC",
            token_out="",
        )
        store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=False)
        proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

        result = await proc.drain_one(led_id)

        assert result is True
        store.save_accounting_event.assert_awaited_once()
        written_event = store.save_accounting_event.call_args[0][0]
        assert isinstance(written_event, PerpAccountingEvent)
        assert written_event.event_type == PerpEventType.PERP_CLOSE.value


class TestDrainOneWritesVaultEvent:
    @pytest.mark.asyncio
    async def test_drain_one_writes_vault_deposit_event(self) -> None:
        """drain_one on VAULT_DEPOSIT outbox row produces a VaultAccountingEvent, not None."""
        led_id = str(uuid.uuid4())
        position_key = "vault:metamorpho:arbitrum:0xwallet:0xvault"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="VAULT_DEPOSIT",
            position_key=position_key,
            market_id="0xvault",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="VAULT_DEPOSIT",
            protocol="metamorpho",
            chain="arbitrum",
            token_in="USDC",
            token_out="",
            amount_in="500.0",
        )
        store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=False)
        proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

        result = await proc.drain_one(led_id)

        assert result is True, "drain_one must return True for VAULT_DEPOSIT"
        store.save_accounting_event.assert_awaited_once()
        written_event = store.save_accounting_event.call_args[0][0]
        assert isinstance(written_event, VaultAccountingEvent)
        assert written_event.event_type == VaultEventType.VAULT_DEPOSIT.value
        assert written_event.assets_amount == Decimal("500.0")

    @pytest.mark.asyncio
    async def test_drain_one_writes_vault_redeem_event(self) -> None:
        """drain_one on VAULT_REDEEM outbox row produces a VaultAccountingEvent."""
        led_id = str(uuid.uuid4())
        position_key = "vault:metamorpho:arbitrum:0xwallet:0xvault"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="VAULT_REDEEM",
            position_key=position_key,
            market_id="0xvault",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="VAULT_REDEEM",
            protocol="metamorpho",
            chain="arbitrum",
            token_in="USDC",
            token_out="",
            amount_in="250.0",
        )
        store = _make_mock_store(outbox_row=outbox_row, ledger_row=ledger_row, already_written=False)
        proc = AccountingProcessor(state_manager=store, basis_store=FIFOBasisStore(), deployment_id="dep-1")

        result = await proc.drain_one(led_id)

        assert result is True
        store.save_accounting_event.assert_awaited_once()
        written_event = store.save_accounting_event.call_args[0][0]
        assert isinstance(written_event, VaultAccountingEvent)
        # VAULT_REDEEM maps to VAULT_WITHDRAW (matching legacy builder)
        assert written_event.event_type == VaultEventType.VAULT_WITHDRAW.value


# ──────────────────────────────────────────────────────────────────────────────
# Unit tests: handle_lp
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLpOpen:
    def test_basic_lp_open_returns_event(self) -> None:
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0xpool",
            market_id="0xpool",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="USDC",
            token_out="DAI",
            amount_in="100.0",
            amount_out="100.0",
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert isinstance(result, LPAccountingEvent)
        assert result.event_type == LPEventType.LP_OPEN.value
        assert result.token0 == "USDC"
        assert result.token1 == "DAI"
        assert result.pool_address == "0xpool"
        assert result.position_key == "lp:aerodrome:base:0xwallet:0xpool"

    def test_pendle_lp_returns_none(self) -> None:
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="pendle_lp:base:0xwallet:0xmarket",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="pendle",
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is None, "Pendle LP must return None (handled by pendle_handler)"

    def test_non_lp_intent_returns_none(self) -> None:
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(led_id, intent_type="SWAP")
        ledger_row = _make_ledger_row(led_id, intent_type="SWAP")

        assert handle_lp(outbox_row, ledger_row) is None

    def test_lp_open_fallback_amount_from_ledger_row(self) -> None:
        """When no lp_open_data in extracted_data, amount0/1 come from amount_in/out strings."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0xpool",
            market_id="0xpool",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            token_in="USDC",
            token_out="DAI",
            amount_in="250.5",
            amount_out="251.0",
            extracted_data_json="",
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.amount0 == Decimal("250.5")
        assert result.amount1 == Decimal("251.0")

    def test_missing_pool_address_returns_none(self) -> None:
        """Without position_key and without market_id, pool cannot be resolved → None."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="",  # empty — no last segment
            market_id="",
        )
        ledger_row = _make_ledger_row(led_id, intent_type="LP_OPEN", protocol="aerodrome")

        result = handle_lp(outbox_row, ledger_row)

        assert result is None

    def test_lp_close_returns_event(self) -> None:
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0xpooladdr",
            market_id="0xpooladdr",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.event_type == LPEventType.LP_CLOSE.value
        assert result.pool_address == "0xpooladdr"

    def test_lp_open_with_lp_open_data_and_resolver(self) -> None:
        """LPOpenData in extracted_data_json with a mocked token resolver scales raw ints."""
        from unittest.mock import patch

        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0xpool",
            market_id="0xpool",
        )
        # Build a serialized LPOpenData in extracted_data_json.
        extracted = json.dumps({
            "lp_open_data": {
                "_type": "LPOpenData",
                "position_id": 42,
                "amount0": "100000000",   # 100 USDC (6 dec)
                "amount1": "50000000000000000000",  # 50 DAI (18 dec)
                "tick_lower": None,
                "tick_upper": None,
                "liquidity": None,
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="USDC",
            token_out="DAI",
            extracted_data_json=extracted,
        )

        mock_ti_usdc = MagicMock()
        mock_ti_usdc.decimals = 6
        mock_ti_dai = MagicMock()
        mock_ti_dai.decimals = 18

        def _resolve(token: str, chain: str = "") -> Any:
            return mock_ti_usdc if token == "USDC" else mock_ti_dai

        mock_resolver = MagicMock(resolve=MagicMock(side_effect=_resolve))

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=mock_resolver):
            result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        # amount0 = 100_000_000 / 1e6 = 100
        assert result.amount0 == Decimal("100")
        # amount1 = 50_000_000_000_000_000_000 / 1e18 = 50
        assert result.amount1 == Decimal("50")

    def test_pool_address_parsed_from_multi_segment_position_key(self) -> None:
        """Pool address is always the last ':' segment of position_key."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:curve:mainnet:0xwallet:0xstablepool",
            market_id="",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="curve",
            chain="mainnet",
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.pool_address == "0xstablepool"


# ──────────────────────────────────────────────────────────────────────────────
# Unit tests: handle_perp
# ──────────────────────────────────────────────────────────────────────────────


class TestHandlePerp:
    def test_perp_open_returns_event(self) -> None:
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="PERP_OPEN",
            position_key="perp:gmx_v2:arbitrum:0xwallet:eth/usd",
            market_id="eth/usd",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="PERP_OPEN",
            protocol="gmx_v2",
            chain="arbitrum",
            token_in="USDC",
            token_out="",
            amount_in="500.0",
        )

        result = handle_perp(outbox_row, ledger_row)

        assert result is not None
        assert isinstance(result, PerpAccountingEvent)
        assert result.event_type == PerpEventType.PERP_OPEN.value
        assert result.collateral_token == "USDC"
        assert result.collateral_amount == Decimal("500.0")
        assert result.market == "eth/usd"

    def test_perp_close_returns_event(self) -> None:
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="PERP_CLOSE",
            position_key="perp:gmx_v2:arbitrum:0xwallet:eth/usd",
            market_id="eth/usd",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="PERP_CLOSE",
            protocol="gmx_v2",
            chain="arbitrum",
            token_in="USDC",
        )

        result = handle_perp(outbox_row, ledger_row)

        assert result is not None
        assert result.event_type == PerpEventType.PERP_CLOSE.value

    def test_non_perp_returns_none(self) -> None:
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(led_id, intent_type="SWAP")
        ledger_row = _make_ledger_row(led_id, intent_type="SWAP")

        assert handle_perp(outbox_row, ledger_row) is None

    def test_perp_open_confidence_is_estimated(self) -> None:
        """Perp events are always ESTIMATED until a receipt parser is wired."""
        from almanak.framework.accounting.models import AccountingConfidence

        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="PERP_OPEN",
            position_key="perp:gmx_v2:arbitrum:0xwallet:btc/usd",
            market_id="btc/usd",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="PERP_OPEN",
            protocol="gmx_v2",
            chain="arbitrum",
            token_in="USDC",
            amount_in="1000.0",
        )

        result = handle_perp(outbox_row, ledger_row)

        assert result is not None
        assert result.confidence == AccountingConfidence.ESTIMATED

    def test_market_falls_back_to_position_key_last_segment(self) -> None:
        """When market_id is empty, market is parsed from the last ':' of position_key."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="PERP_OPEN",
            position_key="perp:drift:solana:0xwallet:sol-perp",
            market_id="",  # empty market_id
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="PERP_OPEN",
            protocol="drift",
            chain="solana",
            token_in="SOL",
        )

        result = handle_perp(outbox_row, ledger_row)

        assert result is not None
        assert result.market == "sol-perp"

    def test_perp_open_with_perp_data_in_extracted(self) -> None:
        """PerpData fields (leverage, entry_price) are extracted when present."""
        from unittest.mock import patch

        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="PERP_OPEN",
            position_key="perp:gmx_v2:arbitrum:0xwallet:eth/usd",
            market_id="eth/usd",
        )
        extracted = json.dumps({
            "perp_data": {
                "_type": "PerpData",
                "position_id": "123",
                "size_delta": "5000",
                "collateral": "500000000",
                "entry_price": "3000.0",
                "exit_price": None,
                "leverage": "10.0",
                "realized_pnl": None,
                "fees_paid": None,
                "funding_fee_usd": None,
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="PERP_OPEN",
            protocol="gmx_v2",
            chain="arbitrum",
            token_in="USDC",
            amount_in="500.0",
            extracted_data_json=extracted,
        )

        result = handle_perp(outbox_row, ledger_row)

        assert result is not None
        assert result.entry_price == Decimal("3000.0")
        assert result.leverage == Decimal("10.0")


# ──────────────────────────────────────────────────────────────────────────────
# Unit tests: handle_vault
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleVault:
    def test_vault_deposit_returns_event(self) -> None:
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="VAULT_DEPOSIT",
            position_key="vault:metamorpho:arbitrum:0xwallet:0xvault",
            market_id="0xvault",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="VAULT_DEPOSIT",
            protocol="metamorpho",
            chain="arbitrum",
            token_in="USDC",
            token_out="",
            amount_in="500.0",
        )

        result = handle_vault(outbox_row, ledger_row)

        assert result is not None
        assert isinstance(result, VaultAccountingEvent)
        assert result.event_type == VaultEventType.VAULT_DEPOSIT.value
        assert result.asset_token == "USDC"
        assert result.assets_amount == Decimal("500.0")
        assert result.vault_address == "0xvault"

    def test_vault_redeem_maps_to_vault_withdraw(self) -> None:
        """VAULT_REDEEM intent should produce event_type=VAULT_WITHDRAW (legacy builder parity)."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="VAULT_REDEEM",
            position_key="vault:metamorpho:arbitrum:0xwallet:0xvault",
            market_id="0xvault",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="VAULT_REDEEM",
            protocol="metamorpho",
            chain="arbitrum",
            token_in="USDC",
            amount_in="100.0",
        )

        result = handle_vault(outbox_row, ledger_row)

        assert result is not None
        # Matches old builder: VaultEventType.VAULT_WITHDRAW
        assert result.event_type == VaultEventType.VAULT_WITHDRAW.value

    def test_vault_redeem_all_string_returns_none_amount(self) -> None:
        """Amount 'all' (close entire position) should leave assets_amount as None."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="VAULT_REDEEM",
            position_key="vault:metamorpho:arbitrum:0xwallet:0xvault",
            market_id="0xvault",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="VAULT_REDEEM",
            protocol="metamorpho",
            chain="arbitrum",
            token_in="USDC",
            amount_in="all",
        )

        result = handle_vault(outbox_row, ledger_row)

        assert result is not None
        assert result.assets_amount is None

    def test_non_vault_returns_none(self) -> None:
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(led_id, intent_type="SWAP")
        ledger_row = _make_ledger_row(led_id, intent_type="SWAP")

        assert handle_vault(outbox_row, ledger_row) is None

    def test_vault_deposit_confidence_is_estimated(self) -> None:
        from almanak.framework.accounting.models import AccountingConfidence

        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="VAULT_DEPOSIT",
            position_key="vault:metamorpho:arbitrum:0xwallet:0xvault",
            market_id="0xvault",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="VAULT_DEPOSIT",
            protocol="metamorpho",
            chain="arbitrum",
            token_in="USDC",
            amount_in="100.0",
        )

        result = handle_vault(outbox_row, ledger_row)

        assert result is not None
        assert result.confidence == AccountingConfidence.ESTIMATED

    def test_vault_address_falls_back_to_position_key_last_segment(self) -> None:
        """When market_id is empty, vault_address is parsed from the last ':' segment."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="VAULT_DEPOSIT",
            position_key="vault:yearn:mainnet:0xwallet:0xyvault",
            market_id="",  # empty market_id
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="VAULT_DEPOSIT",
            protocol="yearn",
            chain="mainnet",
            token_in="DAI",
            amount_in="1000.0",
        )

        result = handle_vault(outbox_row, ledger_row)

        assert result is not None
        assert result.vault_address == "0xyvault"

    def test_vault_deposit_shares_and_price_are_none(self) -> None:
        """shares_amount, share_price, yield_usd are None until vault receipt parser is wired."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="VAULT_DEPOSIT",
            position_key="vault:metamorpho:arbitrum:0xwallet:0xvault",
            market_id="0xvault",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="VAULT_DEPOSIT",
            protocol="metamorpho",
            chain="arbitrum",
            token_in="USDC",
            amount_in="100.0",
        )

        result = handle_vault(outbox_row, ledger_row)

        assert result is not None
        assert result.shares_amount is None
        assert result.share_price is None
        assert result.yield_usd is None
