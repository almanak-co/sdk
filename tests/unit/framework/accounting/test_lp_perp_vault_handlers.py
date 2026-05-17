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
        position_key = "lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            wallet_address="0xwallet",
            position_key=position_key,
            market_id="0x1111111111111111111111111111111111111111",
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
        assert written_event.pool_address == "0x1111111111111111111111111111111111111111"

    @pytest.mark.asyncio
    async def test_drain_one_writes_lp_close_event(self) -> None:
        """drain_one on LP_CLOSE outbox row produces an LPAccountingEvent."""
        led_id = str(uuid.uuid4())
        position_key = "lp:uniswap_v3:arbitrum:0xwallet:0x2222222222222222222222222222222222222222"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key=position_key,
            market_id="0x2222222222222222222222222222222222222222",
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
            position_key="lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
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
        assert result.pool_address == "0x1111111111111111111111111111111111111111"
        assert result.position_key == "lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111"

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
            position_key="lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
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
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x3333333333333333333333333333333333333333",
            market_id="0x3333333333333333333333333333333333333333",
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
        assert result.pool_address == "0x3333333333333333333333333333333333333333"

    def test_lp_open_with_lp_open_data_and_resolver(self) -> None:
        """LPOpenData in extracted_data_json with a mocked token resolver scales raw ints."""
        from unittest.mock import patch

        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
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
            position_key="lp:curve:mainnet:0xwallet:0x4444444444444444444444444444444444444444",
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
        assert result.pool_address == "0x4444444444444444444444444444444444444444"

    @pytest.mark.parametrize(
        ("position_key", "market_id", "expected_pool_address"),
        [
            pytest.param(
                # Aerodrome / V2-family — position-key tail IS the pool address.
                "lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
                "",
                "0x1111111111111111111111111111111111111111",
                id="aerodrome_v2_address_in_position_key",
            ),
            pytest.param(
                # Uniswap V3-style — position-key tail is a token-fee descriptor
                # ("weth/usdc/500"). Pre-VIB-4274 the descriptor was written
                # verbatim into ``accounting_events.pool_address`` (no fallback,
                # silent data corruption). Now the handler detects the ``/`` and
                # falls back to ``outbox_row.market_id``.
                "lp:uniswap_v3:arbitrum:0xwallet:weth/usdc/500",
                "0xc6962004f452be9203591991d15f6b388e09e8d0",
                "0xc6962004f452be9203591991d15f6b388e09e8d0",
                id="uniswap_v3_descriptor_falls_back_to_market_id",
            ),
        ],
    )
    def test_vib4274_resolve_lp_pool_address_descriptor_vs_address(
        self, position_key: str, market_id: str, expected_pool_address: str
    ) -> None:
        """VIB-4274 — descriptor-shaped position-key tails (``weth/usdc/500``)
        MUST NOT be written verbatim into ``accounting_events.pool_address``.

        Aerodrome / V2-family keys end in a real address — used directly.
        Uniswap V3-family keys end in a slash-separated token-fee descriptor —
        the handler falls back to ``outbox_row.market_id`` (which the runner
        populates with the real pool address for these venues).

        Without this guard, ``pool_address`` columns end up with values like
        ``"weth/usdc/500"`` and a downstream join on ``pool_address`` never
        matches the canonical hex address rows in adjacent tables.
        """
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key=position_key,
            market_id=market_id,
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome" if "aerodrome" in position_key else "uniswap_v3",
            chain="base" if "aerodrome" in position_key else "arbitrum",
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.pool_address == expected_pool_address
        # Hard invariant: never the descriptor.
        assert "/" not in result.pool_address

    def test_vib4396_lp_open_reads_pool_address_from_receipt(self) -> None:
        """VIB-4396 — when the runner stamps a descriptor in BOTH the
        position_key tail AND market_id (the live regression trigger), the
        accounting writer must recover the on-chain pool address from the
        receipt parser's ``lp_open_data.pool_address`` (VIB-3893).

        Live evidence on Arbitrum WETH/USDC 0.05%: the pool is
        ``0xc6962004f452be9203591991d15f6b388e09e8d0``; the runner wrote
        ``"weth/usdc/500"`` to ``market_id``; the receipt parser's
        ``lp_open_data.pool_address`` carried the real address. Pre-fix,
        ``accounting_events.pool_address`` ended up as the descriptor.
        """
        led_id = str(uuid.uuid4())
        real_pool = "0xc6962004f452be9203591991d15f6b388e09e8d0"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:weth/usdc/500",
            # Same live bug: market_id also carries the descriptor.
            market_id="weth/usdc/500",
        )
        extracted = json.dumps({
            "lp_open_data": {
                "_type": "LPOpenData",
                "position_id": 5_487_862,
                "pool_address": real_pool,
                "amount0": "871720086157647",
                "amount1": "2402098",
                "tick_lower": -201_280,
                "tick_upper": -197_220,
                "liquidity": "476588196908",
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.pool_address == real_pool
        # Hard invariant from VIB-4274 — never the descriptor.
        assert "/" not in result.pool_address

    def test_vib4396_lp_close_uses_receipt_pool_address(self) -> None:
        """LP_CLOSE: the receipt parser populates ``lp_close_data.pool_address``
        (VIB-3940) from the Burn event's emitter (= the pool). When that's
        present, the handler should prefer it over any descriptor in
        ``market_id`` — same rationale as VIB-4396 OPEN-side.
        """
        led_id = str(uuid.uuid4())
        real_pool = "0xc6962004f452be9203591991d15f6b388e09e8d0"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:weth/usdc/500",
            market_id="weth/usdc/500",
        )
        extracted = json.dumps({
            "lp_close_data": {
                "_type": "LPCloseData",
                "amount0_collected": "871720086157647",
                "amount1_collected": "2402098",
                "fees0": "0",
                "fees1": "0",
                "pool_address": real_pool,
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.pool_address == real_pool
        assert "/" not in result.pool_address

    def test_vib4396_lp_close_falls_back_to_prior_open_pool_address(self) -> None:
        """LP_CLOSE without a receipt-side ``lp_close_data.pool_address``
        (pre-VIB-3940 parser, or fee-only collect with no Burn) must fall
        back to the prior OPEN payload's ``pool_address``.

        This keeps the bookkeeping coherent across the OPEN → CLOSE lifecycle
        when only the OPEN-side receipt carried the canonical address.
        """
        led_id = str(uuid.uuid4())
        real_pool = "0xc6962004f452be9203591991d15f6b388e09e8d0"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:weth/usdc/500",
            market_id="weth/usdc/500",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json="",
        )
        prior_open = {"pool_address": real_pool}

        result = handle_lp(outbox_row, ledger_row, prior_open_payload=prior_open)

        assert result is not None
        assert result.pool_address == real_pool

    def test_vib4396_lp_close_recovers_from_semantic_grouping_key(self) -> None:
        """When the prior OPEN payload itself was written under the
        pre-VIB-4396 regime (``pool_address`` = descriptor), recover the
        canonical address from ``position_reference.semantic_grouping_key``
        (= ``"chain:0x1111111111111111111111111111111111111111"``). Closes the migration window for in-flight
        OPENs whose accounting rows already carry the descriptor leak.
        """
        led_id = str(uuid.uuid4())
        real_pool = "0xc6962004f452be9203591991d15f6b388e09e8d0"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:weth/usdc/500",
            market_id="weth/usdc/500",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json="",
        )
        # Prior OPEN's payload carries the descriptor (pre-fix data) AND
        # the position_reference (always written correctly via VIB-4262).
        prior_open = {
            "pool_address": "weth/usdc/500",
            "position_reference": {
                "semantic_grouping_key": f"arbitrum:{real_pool}",
            },
        }

        result = handle_lp(outbox_row, ledger_row, prior_open_payload=prior_open)

        assert result is not None
        assert result.pool_address == real_pool

    def test_vib4396_market_id_descriptor_rejected_when_no_receipt(self) -> None:
        """No receipt, no prior payload, position_key tail is a descriptor,
        market_id is a descriptor — every source is descriptor-shaped, so the
        handler must drop the event rather than stamp a descriptor into
        ``pool_address``. This is the strict fail-closed lower bound that
        guarantees a descriptor cannot leak via any path.
        """
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:weth/usdc/500",
            market_id="weth/usdc/500",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
        )

        assert handle_lp(outbox_row, ledger_row) is None

    def test_vib4396_classic_aerodrome_solidly_descriptor_accepted(self) -> None:
        """Codex P1 on PR #2289 — classic Aerodrome (Solidly fork) LPs have
        no on-chain pool address surfaced through the receipt parser path.

        The runner stamps ``TOKEN0/TOKEN1/{stable|volatile}`` into BOTH
        ``position_key`` tail and ``market_id`` because that descriptor IS
        the only stable position identifier the protocol surfaces — there
        is no NPM-managed pool address. Prior to the Codex catch, the
        new ``_clean`` filter rejected ALL slash-containing values, so
        every classic-Aerodrome LP_OPEN was dropped before reaching the
        accounting layer (silent regression — events vanish entirely).

        The post-fix contract: a slash-containing value whose last segment
        is alphabetic (``stable``/``volatile``) is a canonical Solidly
        descriptor — accept. A slash-containing value whose last segment
        is numeric (V3 fee tier like ``500``) is a V3 descriptor — reject.
        """
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:usdc/dai/stable",
            market_id="usdc/dai/stable",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="USDC",
            token_out="DAI",
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None, (
            "classic Aerodrome LP_OPEN must NOT be dropped — the "
            "Solidly-style descriptor IS the canonical pool identifier"
        )
        assert result.pool_address == "usdc/dai/stable"

    def test_vib4396_classic_aerodrome_volatile_descriptor_accepted(self) -> None:
        """Symmetric Codex P1 guard for the ``volatile`` pool variant."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:weth/usdc/volatile",
            market_id="weth/usdc/volatile",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="WETH",
            token_out="USDC",
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.pool_address == "weth/usdc/volatile"

    def test_vib4396_extracted_data_dict_fallback_path(self) -> None:
        """gemini HIGH on PR #2289 — ``deserialize_extracted_data`` returns
        a plain dict (with ``_type`` re-added) when dataclass reconstruction
        fails, e.g. when a writer-side schema mismatch trips
        ``_reconstruct_dataclass`` (``ledger.py:943-945``). The resolver
        must read ``pool_address`` from either path — the original
        implementation used ``getattr`` only, which silently returned ``""``
        for the dict fallback and dropped the chain-extracted pool address
        on the floor.

        Simulated here by passing a payload with an unrecognised ``_type``
        — the deserialiser leaves it as a dict (the documented fallback).
        """
        led_id = str(uuid.uuid4())
        real_pool = "0xc6962004f452be9203591991d15f6b388e09e8d0"
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:weth/usdc/500",
            market_id="weth/usdc/500",
        )
        # Unrecognised ``_type`` forces the dict fallback in
        # ``deserialize_extracted_data``.
        extracted = json.dumps({
            "lp_open_data": {
                "_type": "UnknownVariant",
                "pool_address": real_pool,
                "amount0": "1",
                "amount1": "1",
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None, (
            "dict-fallback ``lp_open_data`` must still surface "
            "pool_address — gemini HIGH on PR #2289"
        )
        assert result.pool_address == real_pool

    def test_vib4396_round_trip_open_close_pool_address_matches(self) -> None:
        """VIB-4396 round-trip — the OPEN's ``pool_address`` and the matching
        CLOSE's ``pool_address`` MUST be identical so downstream lot-matching
        reconciles them as the same logical position. This is the actual
        operator-trust invariant the resolver fix exists to produce; the
        priority-by-priority unit tests above verify each source in
        isolation but never prove the OPEN↔CLOSE pairing.

        Live regression scenario: runner stamps the descriptor in
        ``market_id`` AND ``position_key`` for both the OPEN and the CLOSE
        (as it does on Uniswap V3 today). Pre-fix, OPEN landed with the
        descriptor and CLOSE landed with the on-chain address (because the
        close-side resolver hit a different code path) → lot matching
        silently broke. Post-fix, both sides resolve to the canonical
        on-chain address.
        """
        real_pool = "0xc6962004f452be9203591991d15f6b388e09e8d0"
        position_key = "lp:uniswap_v3:arbitrum:0xwallet:weth/usdc/500"

        # OPEN side — receipt-side priority 1.
        open_id = str(uuid.uuid4())
        open_outbox = _make_outbox_row(
            open_id,
            intent_type="LP_OPEN",
            position_key=position_key,
            market_id="weth/usdc/500",
        )
        open_extracted = json.dumps({
            "lp_open_data": {
                "_type": "LPOpenData",
                "position_id": 5_487_862,
                "pool_address": real_pool,
                "amount0": "871720086157647",
                "amount1": "2402098",
                "tick_lower": -201_280,
                "tick_upper": -197_220,
                "liquidity": "476588196908",
            }
        })
        open_ledger = _make_ledger_row(
            open_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=open_extracted,
        )
        open_event = handle_lp(open_outbox, open_ledger)

        # CLOSE side — receipt-side priority 1 (VIB-3940).
        close_id = str(uuid.uuid4())
        close_outbox = _make_outbox_row(
            close_id,
            intent_type="LP_CLOSE",
            position_key=position_key,
            market_id="weth/usdc/500",
        )
        close_extracted = json.dumps({
            "lp_close_data": {
                "_type": "LPCloseData",
                "amount0_collected": "871720086157647",
                "amount1_collected": "2402098",
                "fees0": "0",
                "fees1": "0",
                "pool_address": real_pool,
            }
        })
        close_ledger = _make_ledger_row(
            close_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=close_extracted,
        )
        close_event = handle_lp(close_outbox, close_ledger)

        assert open_event is not None
        assert close_event is not None
        assert open_event.pool_address == close_event.pool_address == real_pool
        # Hard invariant: descriptor never reaches the payload on either side.
        assert "/" not in open_event.pool_address
        assert "/" not in close_event.pool_address

    def test_vib4396_round_trip_open_receipt_close_via_prior_open_matches(self) -> None:
        """Mixed-priority round-trip — OPEN resolves via receipt (priority 1),
        CLOSE resolves via prior-OPEN payload (priority 2, e.g. pre-VIB-3940
        parser path or fee-only collect with no Burn event). Both sides
        MUST still produce identical ``pool_address``.
        """
        real_pool = "0xc6962004f452be9203591991d15f6b388e09e8d0"
        position_key = "lp:uniswap_v3:arbitrum:0xwallet:weth/usdc/500"

        # OPEN — receipt priority 1.
        open_id = str(uuid.uuid4())
        open_outbox = _make_outbox_row(
            open_id,
            intent_type="LP_OPEN",
            position_key=position_key,
            market_id="weth/usdc/500",
        )
        open_extracted = json.dumps({
            "lp_open_data": {
                "_type": "LPOpenData",
                "position_id": 5_487_862,
                "pool_address": real_pool,
                "amount0": "1",
                "amount1": "1",
                "tick_lower": -100,
                "tick_upper": 100,
                "liquidity": "1",
            }
        })
        open_ledger = _make_ledger_row(
            open_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            extracted_data_json=open_extracted,
        )
        open_event = handle_lp(open_outbox, open_ledger)

        # CLOSE — no receipt extraction; recover via prior_open_payload.
        close_id = str(uuid.uuid4())
        close_outbox = _make_outbox_row(
            close_id,
            intent_type="LP_CLOSE",
            position_key=position_key,
            market_id="weth/usdc/500",
        )
        close_ledger = _make_ledger_row(
            close_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            extracted_data_json="",
        )
        assert open_event is not None
        prior_open_payload = {"pool_address": open_event.pool_address}
        close_event = handle_lp(
            close_outbox, close_ledger, prior_open_payload=prior_open_payload,
        )

        assert close_event is not None
        assert open_event.pool_address == close_event.pool_address == real_pool

    def test_vib3893_lp_open_propagates_tick_metadata_and_in_range(self) -> None:
        """VIB-3893 — tick_lower/upper/liquidity/current_tick from
        ``lp_open_data`` and derived ``in_range`` end up on the
        accounting payload. Pre-fix the LP_OPEN accounting_event omitted
        these even though the receipt parser populated them on
        ``lp_open_data`` — the dashboard's Trade Tape rendered "in_range
        UNKNOWN" on every production LP open."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        # Receipt-parser shape: position_id, amounts, ticks, liquidity, and
        # the slot0-derived current_tick. Bracket [-1000, +1000] with
        # current_tick=0 should mark in_range=True.
        extracted = json.dumps({
            "lp_open_data": {
                "_type": "LPOpenData",
                "position_id": 5464864,
                "amount0": "1000000",
                "amount1": "1000000000000000000",
                "tick_lower": -1000,
                "tick_upper": 1000,
                "liquidity": 12345678901234,
                "current_tick": 0,
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.tick_lower == -1000
        assert result.tick_upper == 1000
        assert result.liquidity == 12345678901234
        assert result.current_tick == 0
        assert result.in_range is True

        # The serialized payload (what the writer persists) carries them too.
        payload = json.loads(result.to_payload_json())
        assert payload["tick_lower"] == -1000
        assert payload["tick_upper"] == 1000
        assert payload["current_tick"] == 0
        assert payload["liquidity"] == 12345678901234
        assert payload["in_range"] is True

    def test_vib3940_lp_close_propagates_current_tick_and_in_range(self) -> None:
        """VIB-3940 — lane-symmetry sibling of VIB-3893. ``LPCloseData.current_tick``
        (sourced from a Swap event in the close receipt or the runner's
        slot0 fallback) plus ``tick_lower``/``tick_upper`` backfilled from
        the prior OPEN must produce a non-null ``in_range`` on the LP_CLOSE
        accounting event. Pre-fix the LP_CLOSE event always carried
        ``current_tick=None`` and ``in_range=None`` regardless of pool
        state — Q4 (LP composition shift) couldn't answer "was the position
        in-range at close?" without a separate on-chain query."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        # Receipt-parser shape for a close: principal collected, fees, the
        # liquidity removed, plus the new VIB-3940 fields current_tick +
        # pool_address. Bracket [-1000, +1000] with current_tick=0 ⇒ in_range=True.
        extracted = json.dumps({
            "lp_close_data": {
                "_type": "LPCloseData",
                "amount0_collected": "1000000",
                "amount1_collected": "1000000000000000000",
                "fees0": "0",
                "fees1": "0",
                "liquidity_removed": "12345678901234",
                "current_tick": 0,
                "pool_address": "0x1111111111111111111111111111111111111111",
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
        )
        # Bracket comes from the prior OPEN — the close receipt does not
        # re-emit it. Pass it explicitly via the handler's prior_open_payload
        # parameter (the production processor wires this from the prior
        # accounting_event row for the same position_key).
        prior_open = {
            "tick_lower": -1000,
            "tick_upper": 1000,
            "liquidity": 12345678901234,
            "cost_basis_usd": "100.0",
        }

        result = handle_lp(outbox_row, ledger_row, prior_open_payload=prior_open)

        assert result is not None
        assert result.tick_lower == -1000
        assert result.tick_upper == 1000
        assert result.current_tick == 0
        assert result.in_range is True, (
            f"VIB-3940: LP_CLOSE in_range must derive to True when -1000 <= 0 < 1000; "
            f"got {result.in_range!r}"
        )

        # Serialized payload carries the fields too — Trade Tape reads from JSON.
        payload = json.loads(result.to_payload_json())
        assert payload["current_tick"] == 0
        assert payload["in_range"] is True

    def test_vib3940_lp_close_in_range_false_when_current_tick_outside_bracket(self) -> None:
        """Same half-open convention as LP_OPEN: ``tick_lower <= current_tick <
        tick_upper``. ``current_tick == tick_upper`` is OUT-of-range."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        extracted = json.dumps({
            "lp_close_data": {
                "_type": "LPCloseData",
                "amount0_collected": "1000000",
                "amount1_collected": "1000000000000000000",
                "fees0": "0",
                "fees1": "0",
                "liquidity_removed": "12345",
                "current_tick": 1000,  # equal to tick_upper -> OUT
                "pool_address": "0x1111111111111111111111111111111111111111",
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
        )
        prior_open = {"tick_lower": -1000, "tick_upper": 1000, "cost_basis_usd": "100.0"}

        result = handle_lp(outbox_row, ledger_row, prior_open_payload=prior_open)

        assert result is not None
        assert result.in_range is False

    def test_vib3940_lp_close_in_range_none_when_current_tick_missing(self) -> None:
        """When the receipt has no Swap event AND the slot0 fallback couldn't
        run (no gateway / no pool_address), ``current_tick`` stays None and
        ``in_range`` MUST stay None — never default to a guess. This is the
        degraded-but-honest path the framework already takes pre-VIB-3940."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        extracted = json.dumps({
            "lp_close_data": {
                "_type": "LPCloseData",
                "amount0_collected": "1000000",
                "amount1_collected": "1000000000000000000",
                "fees0": "0",
                "fees1": "0",
                "liquidity_removed": "12345",
                # current_tick deliberately omitted (defaults to None)
                # pool_address omitted too — no slot0 fallback would fire
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
        )
        prior_open = {"tick_lower": -1000, "tick_upper": 1000, "cost_basis_usd": "100.0"}

        result = handle_lp(outbox_row, ledger_row, prior_open_payload=prior_open)

        assert result is not None
        assert result.current_tick is None
        assert result.in_range is None

    def test_vib3893_in_range_false_when_current_tick_outside_bracket(self) -> None:
        """``in_range`` is half-open ``tick_lower <= current_tick <
        tick_upper`` per VIB-3887. A current_tick equal to tick_upper
        is OUT-of-range — locks the half-open convention."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        extracted = json.dumps({
            "lp_open_data": {
                "_type": "LPOpenData",
                "position_id": 5464864,
                "amount0": "1000000",
                "amount1": "1000000000000000000",
                "tick_lower": -1000,
                "tick_upper": 1000,
                "liquidity": 12345,
                "current_tick": 1000,  # equal to tick_upper -> OUT
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
        )

        result = handle_lp(outbox_row, ledger_row)
        assert result is not None
        assert result.in_range is False

    def test_vib3893_in_range_none_when_current_tick_missing(self) -> None:
        """When ``current_tick`` is unavailable (no slot0 fallback), the
        handler emits ``in_range=None`` — distinct from ``False``. The
        dashboard treats None as "unknown" and renders honest copy."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        extracted = json.dumps({
            "lp_open_data": {
                "_type": "LPOpenData",
                "position_id": 5464864,
                "amount0": "1000000",
                "amount1": "1000000000000000000",
                "tick_lower": -1000,
                "tick_upper": 1000,
                "liquidity": 12345,
                "current_tick": None,
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
        )

        result = handle_lp(outbox_row, ledger_row)
        assert result is not None
        assert result.tick_lower == -1000
        assert result.tick_upper == 1000
        assert result.current_tick is None
        assert result.in_range is None


# ──────────────────────────────────────────────────────────────────────────────
# VIB-3756: cost_basis_usd computation from price_inputs_json
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLpCostBasisUsd:
    """Regression: LP_OPEN events used to hard-code ``cost_basis_usd=None`` so
    LP NFT mints rendered as deployed_usd=$0 in QA dashboards. The handler
    now sums ``token0_amount * price0 + token1_amount * price1`` from the
    ``price_inputs_json`` captured at execution time (matches swap_handler).
    """

    def test_lp_open_with_both_token_prices_computes_cost_basis(self) -> None:
        """Happy path: both prices present, decimals known → HIGH confidence."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        # USDC ≈ $1.00, WETH ≈ $3000.00; 100 USDC + 0.05 WETH = $250.
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="USDC",
            token_out="WETH",
            amount_in="100.0",
            amount_out="0.05",
            price_inputs_json=json.dumps({"USDC": "1.00", "WETH": "3000.00"}),
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.cost_basis_usd == Decimal("250.00")
        assert result.confidence.value == "HIGH"
        assert result.unavailable_reason == ""

    def test_lp_open_with_one_token_unpriced_returns_none_not_zero(self) -> None:
        """Per repo rule: wrong is worse than absent. Missing price for one leg
        returns ``cost_basis_usd=None`` and a structured reason — NOT $0.
        """
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        # WETH is missing from the oracle.
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="USDC",
            token_out="WETH",
            amount_in="100.0",
            amount_out="0.05",
            price_inputs_json=json.dumps({"USDC": "1.00"}),
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.cost_basis_usd is None, "Missing leg price must produce None, not 0"
        assert "WETH" in result.unavailable_reason
        # VIB-3886: pricing failure degrades confidence to ESTIMATED so
        # downstream consumers (Accountant Test G6, dashboard cells) can
        # tell the USD field is incomplete. Pre-VIB-3886 the LP path
        # contradicted itself with HIGH+unavailable_reason simultaneously.
        assert result.confidence.value == "ESTIMATED"

    def test_lp_open_with_no_price_inputs_returns_none(self) -> None:
        """Empty price_inputs_json (older ledger rows / paper trading) → None."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
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
            price_inputs_json="",
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.cost_basis_usd is None
        assert "no price_inputs_json" in result.unavailable_reason

    def test_lp_open_case_insensitive_price_lookup(self) -> None:
        """token_in/out are uppercased by the handler; price oracle stores upper symbols."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        # Lowercase tokens in the row, uppercase in the oracle.
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="usdc",  # gets uppercased to USDC
            token_out="dai",  # → DAI
            amount_in="100.0",
            amount_out="100.0",
            price_inputs_json=json.dumps({"USDC": "1.00", "DAI": "1.00"}),
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.cost_basis_usd == Decimal("200.00")

    def test_lp_close_with_prices_also_computes_value(self) -> None:
        """LP_CLOSE re-uses cost_basis_usd as the exit value at the close event."""
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x2222222222222222222222222222222222222222",
            market_id="0x2222222222222222222222222222222222222222",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            amount_in="120.0",
            amount_out="0.04",
            price_inputs_json=json.dumps({"USDC": "1.00", "WETH": "3000.00"}),
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.event_type == LPEventType.LP_CLOSE.value
        # 120 USDC + 0.04 * 3000 WETH = 240
        assert result.cost_basis_usd == Decimal("240.00")

    def test_lp_open_with_decimals_assumed_skips_pricing(self) -> None:
        """When token decimals had to be assumed (resolver miss) we still skip
        pricing — amounts may be off by 1e12 for 6-decimal tokens, so a
        confidently wrong USD figure is worse than None.
        """
        from unittest.mock import patch

        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        # extracted_data_json forces the resolver path; resolver returns None
        # → assumed_decimals = True.
        extracted = json.dumps({
            "lp_open_data": {
                "_type": "LPOpenData",
                "amount0": "100000000",
                "amount1": "50000000000000000000",
            }
        })
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="WEIRD",
            token_out="UNKNOWN",
            extracted_data_json=extracted,
            price_inputs_json=json.dumps({"WEIRD": "1.0", "UNKNOWN": "1.0"}),
        )

        # Resolver returns None → assumed_decimals=True
        mock_resolver = MagicMock(resolve=MagicMock(return_value=None))

        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=mock_resolver):
            result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        # Pricing intentionally skipped because decimals are unreliable.
        assert result.cost_basis_usd is None
        assert result.confidence.value == "ESTIMATED"

    def test_lp_open_zero_amount_legs_returns_none(self) -> None:
        """Both amounts empty/zero → ``_compute_cost_basis`` returns None
        (not a concrete zero basis — there were no legs to price).
        """
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="USDC",
            token_out="DAI",
            amount_in="",  # empty
            amount_out="",
            price_inputs_json=json.dumps({"USDC": "1.00", "DAI": "1.00"}),
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.cost_basis_usd is None
        assert result.unavailable_reason is not None
        assert "no resolvable amount legs" in result.unavailable_reason

    def test_lp_open_with_invalid_price_returns_none_and_reason(self) -> None:
        """``price_inputs_json`` carries a non-numeric string → fail-closed
        with an "invalid prices" reason, distinct from the "missing prices"
        bucket. Operators triaging a $None deployed_usd column need to know
        whether the producer dropped the price entirely or wrote a bad one.
        """
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="USDC",  # noqa: S106 — token symbol, not a credential
            token_out="WETH",  # noqa: S106 — token symbol, not a credential
            amount_in="100000000",
            amount_out="50000000000000000000",
            price_inputs_json=json.dumps({"USDC": "abc", "WETH": "3000.00"}),
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.cost_basis_usd is None
        # VIB-3886: invalid-price degrades confidence to ESTIMATED, same as
        # the missing-price case. The HIGH+unavailable_reason contradiction
        # was the regression that hid this bug class on the May 2 dashboard.
        assert result.confidence.value == "ESTIMATED"
        assert result.unavailable_reason is not None
        assert "invalid prices" in result.unavailable_reason
        assert "USDC" in result.unavailable_reason

    def test_lp_open_with_nan_price_returns_none_and_reason(self) -> None:
        """``price_inputs_json`` carries a NaN → ``_safe_decimal`` rejects it
        as non-finite. Same fail-closed shape as the "abc" case.
        """
        led_id = str(uuid.uuid4())
        outbox_row = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:aerodrome:base:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        ledger_row = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="aerodrome",
            chain="base",
            token_in="USDC",  # noqa: S106 — token symbol, not a credential
            token_out="WETH",  # noqa: S106 — token symbol, not a credential
            amount_in="100000000",
            amount_out="50000000000000000000",
            price_inputs_json=json.dumps({"USDC": "1.00", "WETH": "NaN"}),
        )

        result = handle_lp(outbox_row, ledger_row)

        assert result is not None
        assert result.cost_basis_usd is None
        assert result.unavailable_reason is not None
        assert "invalid prices" in result.unavailable_reason
        assert "WETH" in result.unavailable_reason


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


# ──────────────────────────────────────────────────────────────────────────────
# VIB-4262 — LP wallet-basis hooks (regression guard)
# ──────────────────────────────────────────────────────────────────────────────


class TestHandleLpWalletBasisHooks:
    """LP_OPEN drains and LP_CLOSE / LP_COLLECT_FEES record on the chain+wallet
    FIFO pool used by SWAP / lending realized-PnL math.

    Pre-VIB-4262, the LP handler ignored `basis_store` entirely. A no-op LP
    round-trip (open → immediately close on a frozen Anvil fork) left the
    wallet's pre-LP token-out lots intact, so a follow-up SWAP that disposed
    the LP-returned tokens couldn't compute realized_pnl_usd. This class is the
    regression guard — every test here MUST fail on a build that drops the
    wallet-basis hooks.
    """

    def _wallet_lot_count(
        self, basis: FIFOBasisStore, deployment_id: str, chain: str, wallet: str, token: str
    ) -> int:
        """Return the number of basis lots for the chain+wallet pool / token."""
        swap_wallet_key = f"swap:{chain.lower()}:{wallet.lower()}"
        key = basis._key(deployment_id, swap_wallet_key, token)
        return len(basis._lots.get(key, []))

    def _wallet_lot_remaining(
        self, basis: FIFOBasisStore, deployment_id: str, chain: str, wallet: str, token: str
    ) -> Decimal:
        swap_wallet_key = f"swap:{chain.lower()}:{wallet.lower()}"
        key = basis._key(deployment_id, swap_wallet_key, token)
        return sum(
            (lot.get("remaining", Decimal("0")) for lot in basis._lots.get(key, [])),
            start=Decimal("0"),
        )

    def test_lp_open_drains_wallet_basis_for_both_tokens(self) -> None:
        """LP_OPEN must call match_swap_disposal for token0 + token1.

        Set-up: pre-mint USDC + WETH lots in the wallet basis pool (mirrors the
        post-SWAP state for an LP fixture round-trip). Then run handle_lp on an
        LP_OPEN row with amount0=USDC, amount1=WETH. Assert the lots are
        drained (remaining < initial).
        """
        led_id = str(uuid.uuid4())
        basis = FIFOBasisStore()
        # Pre-mint wallet inventory: 100 USDC + 0.04 WETH from a prior SWAP.
        basis.record_swap_acquisition(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xwallet",
            token="USDC",
            amount=Decimal("100"),
            cost_usd=Decimal("100"),
            timestamp=datetime.now(UTC),
            lot_id="USDC_INITIAL_LOT",
        )
        basis.record_swap_acquisition(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xwallet",
            token="WETH",
            amount=Decimal("0.04"),
            cost_usd=Decimal("100"),
            timestamp=datetime.now(UTC),
            lot_id="WETH_INITIAL_LOT",
        )

        outbox = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:USDC/WETH/500",
            market_id="0x1111111111111111111111111111111111111111",
        )
        ledger = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            amount_in="50",
            amount_out="0.02",
        )

        before_usdc = self._wallet_lot_remaining(basis, "dep-1", "arbitrum", "0xwallet", "USDC")
        before_weth = self._wallet_lot_remaining(basis, "dep-1", "arbitrum", "0xwallet", "WETH")
        result = handle_lp(outbox, ledger, basis_store=basis)
        after_usdc = self._wallet_lot_remaining(basis, "dep-1", "arbitrum", "0xwallet", "USDC")
        after_weth = self._wallet_lot_remaining(basis, "dep-1", "arbitrum", "0xwallet", "WETH")

        assert result is not None
        # V3 position key tail is "USDC/WETH/500" (descriptor, not address) —
        # pool_address must come from outbox.market_id, not the position-key
        # tail. Locks in the _resolve_lp_pool_address fix (CodeRabbit 2026-05-11).
        assert result.pool_address == "0x1111111111111111111111111111111111111111"
        # Both token legs were drained from the wallet-basis pool.
        assert after_usdc == before_usdc - Decimal("50"), (
            f"USDC remaining: {before_usdc} → {after_usdc}; expected −50"
        )
        assert after_weth == before_weth - Decimal("0.02"), (
            f"WETH remaining: {before_weth} → {after_weth}; expected −0.02"
        )

    def test_lp_close_records_wallet_basis_for_both_tokens_with_fees(self) -> None:
        """LP_CLOSE must call record_swap_acquisition for token0 + token1 + fees.

        Set-up: empty basis pool. Run handle_lp on an LP_CLOSE row with
        amount0/amount1 from the close payload. Assert lots are minted with
        amount + fees combined.
        """
        led_id = str(uuid.uuid4())
        basis = FIFOBasisStore()
        # Need a prior_open_payload so the close has a cost basis to anchor.
        prior_open = {
            "cost_basis_usd": "100",
            "tick_lower": -1000,
            "tick_upper": 1000,
            "liquidity": "1000000",
            "current_tick": 0,
            "in_range": True,
        }

        outbox = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:USDC/WETH/500",
            market_id="0x1111111111111111111111111111111111111111",
        )
        # LP_CLOSE has no token_in/token_out (returns BOTH tokens) — handler
        # falls back to position-key descriptor.
        ledger = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="",
            token_out="",
            amount_in="50",
            amount_out="0.02",
        )

        before_usdc = self._wallet_lot_count(basis, "dep-1", "arbitrum", "0xwallet", "USDC")
        before_weth = self._wallet_lot_count(basis, "dep-1", "arbitrum", "0xwallet", "WETH")
        result = handle_lp(outbox, ledger, prior_open_payload=prior_open, basis_store=basis)
        after_usdc = self._wallet_lot_count(basis, "dep-1", "arbitrum", "0xwallet", "USDC")
        after_weth = self._wallet_lot_count(basis, "dep-1", "arbitrum", "0xwallet", "WETH")

        assert result is not None
        # Both token legs minted exactly one new acquisition lot.
        assert after_usdc == before_usdc + 1
        assert after_weth == before_weth + 1
        # Lot remainings reflect the LP_CLOSE amounts (fees0/fees1 are None
        # in the fallback path so amount alone is recorded).
        assert self._wallet_lot_remaining(basis, "dep-1", "arbitrum", "0xwallet", "USDC") == Decimal("50")
        assert self._wallet_lot_remaining(basis, "dep-1", "arbitrum", "0xwallet", "WETH") == Decimal("0.02")

    def test_full_round_trip_cancels_open_close_for_swap_match(self) -> None:
        """Open + close on the same amounts → swap-key inventory returns to start.

        End-to-end property: a no-op LP cycle leaves the pool with the
        LP_CLOSE-minted acquisition lot available to match a follow-up SWAP.
        Verify a follow-up ``match_swap_disposal`` of the WETH portion returns
        the LP_CLOSE-stamped cost basis.

        WETH pre-mint is intentionally LESS than the LP_OPEN consumption
        (0.01 < 0.02) so the original lot is fully drained by LP_OPEN and the
        follow-up disposal MUST consume the LP_CLOSE-created lot — preventing
        the test from passing with a broken LP_CLOSE record.
        """
        basis = FIFOBasisStore()
        # Pre-mint 100 USDC + 0.01 WETH (post-SWAP state). WETH is intentionally
        # less than the LP_OPEN draw (0.02) so the original lot fully drains.
        basis.record_swap_acquisition(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xwallet",
            token="USDC",
            amount=Decimal("100"),
            cost_usd=Decimal("100"),
            timestamp=datetime.now(UTC),
            lot_id="USDC_INITIAL_LOT",
        )
        basis.record_swap_acquisition(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xwallet",
            token="WETH",
            amount=Decimal("0.01"),
            cost_usd=Decimal("25"),
            timestamp=datetime.now(UTC),
            lot_id="WETH_INITIAL_LOT",
        )

        # Frozen-Anvil pricing: USDC=$1, WETH=$2500 (so 0.02 WETH = $50,
        # 50 USDC = $50, total cost basis = $100, per-leg = $50).
        prices_json = json.dumps({"USDC": "1.00", "WETH": "2500.00"})

        # LP_OPEN consumes 50 USDC + 0.02 WETH.
        open_id = str(uuid.uuid4())
        handle_lp(
            _make_outbox_row(
                open_id,
                intent_type="LP_OPEN",
                position_key="lp:uniswap_v3:arbitrum:0xwallet:USDC/WETH/500",
                market_id="0x1111111111111111111111111111111111111111",
            ),
            _make_ledger_row(
                open_id,
                intent_type="LP_OPEN",
                protocol="uniswap_v3",
                chain="arbitrum",
                token_in="USDC",
                token_out="WETH",
                amount_in="50",
                amount_out="0.02",
                price_inputs_json=prices_json,
            ),
            basis_store=basis,
        )

        # LP_CLOSE returns 50 USDC + 0.02 WETH (Anvil frozen, no fees).
        close_id = str(uuid.uuid4())
        prior_open = {"cost_basis_usd": "100", "tick_lower": -1000, "tick_upper": 1000}
        handle_lp(
            _make_outbox_row(
                close_id,
                intent_type="LP_CLOSE",
                position_key="lp:uniswap_v3:arbitrum:0xwallet:USDC/WETH/500",
                market_id="0x1111111111111111111111111111111111111111",
            ),
            _make_ledger_row(
                close_id,
                intent_type="LP_CLOSE",
                protocol="uniswap_v3",
                chain="arbitrum",
                token_in="",
                token_out="",
                amount_in="50",
                amount_out="0.02",
                price_inputs_json=prices_json,
            ),
            prior_open_payload=prior_open,
            basis_store=basis,
        )

        # Follow-up SWAP WETH→USDC of the LP-returned 0.02 WETH must FIFO-match
        # the LP_CLOSE-minted lot (the pre-mint lot was fully drained by LP_OPEN
        # because pre-mint=0.01 < LP_OPEN draw=0.02).
        cost_basis_consumed, unmatched = basis.match_swap_disposal(
            deployment_id="dep-1",
            position_key="swap:arbitrum:0xwallet",
            token="WETH",
            amount=Decimal("0.02"),
        )
        assert unmatched == Decimal("0"), (
            f"Follow-up SWAP could not match LP-returned WETH; unmatched={unmatched}"
        )
        # LP_CLOSE wrote a 0.02 WETH lot at per-leg-basis = $50 (cost_basis_usd
        # $100 / 2 active legs). The disposal of the full 0.02 must consume
        # exactly that lot at exactly that basis — proves the LP_CLOSE record
        # actually fired and stamped the right cost.
        assert cost_basis_consumed == Decimal("50"), (
            f"LP_CLOSE-minted lot basis was {cost_basis_consumed}; expected $50"
        )

    def test_basis_store_none_is_a_no_op(self) -> None:
        """`basis_store=None` (paper / dry-run) MUST NOT raise — fallthrough."""
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:USDC/WETH/500",
            market_id="0x1111111111111111111111111111111111111111",
        )
        ledger = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            amount_in="50",
            amount_out="0.02",
        )
        # No basis_store argument → explicit None default.
        result = handle_lp(outbox, ledger, basis_store=None)
        assert result is not None  # event still emits; only basis hook is skipped

    def test_lp_collect_fees_records_basis_when_principal_zero(self) -> None:
        """LP_COLLECT_FEES has amount0/amount1 == 0 by design (fees-only event).

        Pre-fix bug (gemini-code-assist 2026-05-11): the hook used
        `if amount0 > 0` which skipped fee-only collections entirely, leaving
        the basis pool empty for those tokens. After fix: per-leg total =
        principal + fees, so a fees-only LP_COLLECT_FEES still mints a basis
        lot equal to the fees.
        """
        led_id = str(uuid.uuid4())
        basis = FIFOBasisStore()

        # Fee-only event: lp_close_data.amount0_collected = amount1_collected = 0,
        # fees0/fees1 > 0 (typical post-VIB-3494 LP_COLLECT_FEES shape).
        class _LpCloseData:
            amount0_collected = 0
            amount1_collected = 0
            fees0 = 1_000_000  # raw, 6 decimals → 1.0 USDC
            fees1 = 5_000_000_000_000_000  # raw, 18 decimals → 0.005 WETH

        outbox = _make_outbox_row(
            led_id,
            intent_type="LP_COLLECT_FEES",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:USDC/WETH/500",
            market_id="0x1111111111111111111111111111111111111111",
        )
        ledger = _make_ledger_row(
            led_id,
            intent_type="LP_COLLECT_FEES",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="",
            token_out="",
            amount_in="",
            amount_out="",
            extracted_data_json="{}",
        )
        from unittest.mock import MagicMock as _MM
        from unittest.mock import patch as _patch

        usdc_info = _MM()
        usdc_info.decimals = 6
        weth_info = _MM()
        weth_info.decimals = 18

        def _resolve(sym, chain=None):
            return {"USDC": usdc_info, "WETH": weth_info}.get(sym.upper())

        resolver = _MM()
        resolver.resolve = _resolve

        with (
            _patch(
                "almanak.framework.observability.ledger.deserialize_extracted_data",
                return_value={"lp_close_data": _LpCloseData()},
            ),
            _patch(
                "almanak.framework.data.tokens.resolver.get_token_resolver",
                return_value=resolver,
            ),
        ):
            result = handle_lp(outbox, ledger, basis_store=basis)

        assert result is not None
        # Both fee legs minted a basis lot — pre-fix, neither did because
        # amount0==0 and amount1==0 short-circuited the per-leg branches.
        usdc_lots = basis._lots.get(basis._key("dep-1", "swap:arbitrum:0xwallet", "USDC"), [])
        weth_lots = basis._lots.get(basis._key("dep-1", "swap:arbitrum:0xwallet", "WETH"), [])
        assert len(usdc_lots) == 1, "USDC fee lot must be minted on LP_COLLECT_FEES"
        assert len(weth_lots) == 1, "WETH fee lot must be minted on LP_COLLECT_FEES"
        # Lot amount = principal (0) + fees (resolved to human decimal).
        assert usdc_lots[0]["amount"] == Decimal("1.0")
        assert weth_lots[0]["amount"] == Decimal("0.005")

    def test_missing_chain_or_wallet_skips_hook_silently(self) -> None:
        """Empty chain or wallet → swap_wallet_key cannot resolve → hook skipped, no fabrication."""
        led_id = str(uuid.uuid4())
        basis = FIFOBasisStore()
        outbox = _make_outbox_row(led_id, intent_type="LP_OPEN", wallet_address="")
        ledger = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="",  # also empty
            token_in="USDC",
            token_out="WETH",
            amount_in="50",
            amount_out="0.02",
        )
        # Force pool resolution to succeed via market_id.
        outbox["market_id"] = "0x1111111111111111111111111111111111111111"
        outbox["position_key"] = "lp:uniswap_v3::0xwallet:0x1111111111111111111111111111111111111111"

        result = handle_lp(outbox, ledger, basis_store=basis)

        assert result is not None
        # No lots minted because swap_wallet_key couldn't resolve.
        assert len(basis._lots) == 0


class TestLpImpermanentLoss:
    """VIB-4319 — ``LP_CLOSE`` (only) emits ``il_usd`` and
    ``hodl_value_usd`` so the Accountant Test LP4 cell can move from XFAIL
    back to PASS. ``il_usd = (cost_basis_usd − fees_total_usd) − V_hodl``
    (negative ⇒ LP lost vs HODL). The frozen baseline at
    ``tests/fixtures/accounting/lp/expected_baseline.sqlite`` and the
    matching schema field at ``payload_schemas.py:LPCloseEventPayload``
    expected this field; the LP close handler pre-fix never emitted it.

    ``LP_COLLECT_FEES`` is deliberately excluded (Codex review on
    PR #2259): a fee-collect leaves principal on-chain so the IL math
    collapses to ``-V_hodl``. See ``_compute_lp_impermanent_loss`` scope
    note.
    """

    @staticmethod
    def _close_inputs(
        *,
        prices: dict[str, str],
        prior_open: dict[str, Any] | None,
        amount0_collected_raw: str = "120000000",  # USDC (6 dec) 120 USDC
        amount1_collected_raw: str = "40000000000000000",  # WETH (18 dec) 0.04
        fees0_raw: str | None = "0",
        fees1_raw: str | None = "0",
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Build outbox + ledger rows with a typed ``LPCloseData`` payload.

        Production close events ALWAYS carry typed ``lp_close_data`` so the
        IL math has explicit ``fees0`` / ``fees1`` to back out of
        ``amount*_collected`` (which carry principal + fees per
        ``execution/extracted_data.py:153``). Tests must mirror that shape
        or they exercise an ambiguous fallback path the writer never sees.

        ``fees{0,1}_raw=None`` drops the field entirely so the typed dict
        omits it — useful for the "unmeasured fees fail-closed" tests.
        """
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        lp_close_data: dict[str, Any] = {
            "_type": "LPCloseData",
            "amount0_collected": amount0_collected_raw,
            "amount1_collected": amount1_collected_raw,
            "liquidity_removed": "1",
        }
        if fees0_raw is not None:
            lp_close_data["fees0"] = fees0_raw
        if fees1_raw is not None:
            lp_close_data["fees1"] = fees1_raw
        extracted = json.dumps({"lp_close_data": lp_close_data})

        ledger = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
            price_inputs_json=json.dumps(prices),
        )
        return outbox, ledger

    @staticmethod
    def _patch_token_resolver(monkeypatch, decimals: dict[str, int]) -> None:
        """Inject a token resolver that returns the supplied decimals map.

        Required because the typed ``lp_close_data`` path resolves token
        decimals via :func:`get_token_resolver` to scale raw integers to
        human-decimal Decimals.
        """
        from unittest.mock import patch

        mock_resolver = MagicMock()

        def _resolve(token: str, chain: str = ""):
            if token in decimals:
                ti = MagicMock()
                ti.decimals = decimals[token]
                return ti
            return None

        mock_resolver.resolve = MagicMock(side_effect=_resolve)
        monkeypatch.setattr(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            lambda: mock_resolver,
        )

    def test_lp_close_emits_il_and_hodl_against_prior_open(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """V_hodl = entry amounts at close-time prices; il = V_lp − V_hodl."""
        # Entry: 100 USDC ($1) + 0.05 WETH ($2000) = $200.
        # Close: prices move to USDC=$1, WETH=$3000 → V_hodl = 100 + 0.05*3000 = $250.
        # Recovered (principal only, zero fees): 120 USDC + 0.04 WETH at close
        # prices = $240 = V_lp. IL = 240 − 250 = −$10 (LP underperformed HODL).
        self._patch_token_resolver(monkeypatch, {"USDC": 6, "WETH": 18})
        prior_open = {
            "event_type": "LP_OPEN",
            "token0": "USDC",
            "token1": "WETH",
            "amount0": "100.0",
            "amount1": "0.05",
            "cost_basis_usd": "200.0",
        }
        outbox, ledger = self._close_inputs(
            prices={"USDC": "1.00", "WETH": "3000.00"},
            prior_open=prior_open,
            amount0_collected_raw="120000000",  # 120 USDC
            amount1_collected_raw="40000000000000000",  # 0.04 WETH
            fees0_raw="0",
            fees1_raw="0",
        )

        result = handle_lp(outbox, ledger, prior_open_payload=prior_open)

        assert result is not None
        assert result.cost_basis_usd == Decimal("240.00")
        assert result.fees_total_usd == Decimal("0")
        assert result.hodl_value_usd == Decimal("250.00")
        assert result.il_usd == Decimal("-10.00")

    def test_lp_close_il_zero_when_prices_unchanged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Measured zero IL — prices identical to entry, LP recovered exactly entry value.

        Per CLAUDE.md ``Empty ≠ Zero``: ``Decimal("0")`` here is a MEASURED
        zero, distinct from ``None`` for "unmeasured".
        """
        self._patch_token_resolver(monkeypatch, {"USDC": 6, "WETH": 18})
        prior_open = {
            "event_type": "LP_OPEN",
            "token0": "USDC",
            "token1": "WETH",
            "amount0": "100.0",
            "amount1": "0.05",
            "cost_basis_usd": "250.0",
        }
        # Close recovers identical entry amounts at identical prices: V_lp == V_hodl == 250.
        outbox, ledger = self._close_inputs(
            prices={"USDC": "1.00", "WETH": "3000.00"},
            prior_open=prior_open,
            amount0_collected_raw="100000000",  # 100 USDC
            amount1_collected_raw="50000000000000000",  # 0.05 WETH
            fees0_raw="0",
            fees1_raw="0",
        )

        result = handle_lp(outbox, ledger, prior_open_payload=prior_open)

        assert result is not None
        assert result.hodl_value_usd == Decimal("250.00")
        assert result.il_usd == Decimal("0")

    def test_lp_close_without_prior_open_emits_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No prior OPEN ⇒ cannot recover entry amounts ⇒ both IL fields are None."""
        self._patch_token_resolver(monkeypatch, {"USDC": 6, "WETH": 18})
        outbox, ledger = self._close_inputs(
            prices={"USDC": "1.00", "WETH": "3000.00"},
            prior_open=None,
        )

        result = handle_lp(outbox, ledger, prior_open_payload=None)

        assert result is not None
        assert result.il_usd is None
        assert result.hodl_value_usd is None

    def test_lp_close_missing_close_price_returns_none_not_zero(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Per CLAUDE.md ``Empty ≠ Zero``: missing close-time price for a
        non-zero entry leg returns ``None`` for both IL and HODL — not a
        fabricated zero. This is the same fail-closed contract as
        ``compute_lp_cost_basis``.
        """
        self._patch_token_resolver(monkeypatch, {"USDC": 6, "WETH": 18})
        prior_open = {
            "event_type": "LP_OPEN",
            "token0": "USDC",
            "token1": "WETH",
            "amount0": "100.0",
            "amount1": "0.05",
            "cost_basis_usd": "200.0",
        }
        # WETH price absent from close-time oracle.
        outbox, ledger = self._close_inputs(
            prices={"USDC": "1.00"},
            prior_open=prior_open,
        )

        result = handle_lp(outbox, ledger, prior_open_payload=prior_open)

        assert result is not None
        assert result.il_usd is None
        assert result.hodl_value_usd is None

    def test_lp_close_unmeasured_v_lp_still_reports_hodl(self) -> None:
        """When ``cost_basis_usd`` (V_lp) is unmeasured (e.g. assumed
        decimals) but V_hodl is fully measurable, emit ``hodl_value_usd``
        anyway. Operators triaging "why is il_usd null?" can still see
        the HODL anchor.
        """
        from unittest.mock import patch

        prior_open = {
            "event_type": "LP_OPEN",
            "token0": "USDC",
            "token1": "WETH",
            "amount0": "100.0",
            "amount1": "0.05",
            "cost_basis_usd": "200.0",
        }
        # Force the close handler down the ``assumed_decimals`` path so
        # cost_basis_usd is skipped. Use the extracted-data resolver miss
        # technique (resolver returns None → assumed_decimals = True).
        extracted = json.dumps({
            "lp_close_data": {
                "_type": "LPCloseData",
                "amount0_collected": "120000000",
                "amount1_collected": "40000000000000000",
                "fees0": "0",
                "fees1": "0",
                "liquidity_removed": "1",
            }
        })

        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        ledger = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
            price_inputs_json=json.dumps({"USDC": "1.00", "WETH": "3000.00"}),
        )

        mock_resolver = MagicMock(resolve=MagicMock(return_value=None))
        with patch("almanak.framework.data.tokens.resolver.get_token_resolver", return_value=mock_resolver):
            result = handle_lp(outbox, ledger, prior_open_payload=prior_open)

        assert result is not None
        # assumed_decimals fired ⇒ cost_basis_usd is None.
        assert result.cost_basis_usd is None
        # V_hodl is still measurable from prior_open amounts + close-time prices.
        assert result.hodl_value_usd == Decimal("250.00")
        # IL is None because V_lp is None.
        assert result.il_usd is None

    def test_lp_open_does_not_emit_il(self) -> None:
        """IL is only defined at unwind; LP_OPEN must not fabricate a value."""
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(
            led_id,
            intent_type="LP_OPEN",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        ledger = _make_ledger_row(
            led_id,
            intent_type="LP_OPEN",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            amount_in="100.0",
            amount_out="0.05",
            price_inputs_json=json.dumps({"USDC": "1.00", "WETH": "3000.00"}),
        )

        result = handle_lp(outbox, ledger, prior_open_payload=None)

        assert result is not None
        assert result.event_type == LPEventType.LP_OPEN.value
        assert result.il_usd is None
        assert result.hodl_value_usd is None

    def test_il_round_trips_through_to_payload_json(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Serializer round-trip: a non-None ``il_usd`` survives ``to_payload_json``
        and re-parses back to the same Decimal. Without this contract, the
        Accountant Test LP4 cell — which reads via ``json_extract(payload_json,
        '$.il_usd')`` — would still see NULL even though the handler computed
        a value. Same round-trip discipline applies to ``hodl_value_usd``.
        """
        self._patch_token_resolver(monkeypatch, {"USDC": 6, "WETH": 18})
        prior_open = {
            "event_type": "LP_OPEN",
            "token0": "USDC",
            "token1": "WETH",
            "amount0": "100.0",
            "amount1": "0.05",
            "cost_basis_usd": "200.0",
        }
        outbox, ledger = self._close_inputs(
            prices={"USDC": "1.00", "WETH": "3000.00"},
            prior_open=prior_open,
            amount0_collected_raw="120000000",  # 120 USDC
            amount1_collected_raw="40000000000000000",  # 0.04 WETH
            fees0_raw="0",
            fees1_raw="0",
        )

        result = handle_lp(outbox, ledger, prior_open_payload=prior_open)
        assert result is not None

        payload = json.loads(result.to_payload_json())
        # Decimal precision can vary depending on whether the math went
        # through cost_basis_usd subtraction (4dp) or pure leg arithmetic
        # (2dp). Compare semantically by re-parsing the JSON-encoded
        # string back to Decimal.
        assert Decimal(payload["il_usd"]) == Decimal("-10")
        assert Decimal(payload["hodl_value_usd"]) == Decimal("250")

    def test_il_round_trips_none_through_to_payload_json(self) -> None:
        """A None ``il_usd`` must serialize as JSON null (NOT the string "None"
        or absent), so the read-side ``json_extract`` returns SQL NULL and
        the LP4 cell correctly classifies the event as "not yet computed".
        """
        outbox, ledger = self._close_inputs(
            prices={"USDC": "1.00", "WETH": "3000.00"},
            prior_open=None,
        )

        result = handle_lp(outbox, ledger, prior_open_payload=None)
        assert result is not None

        payload = json.loads(result.to_payload_json())
        # The key must be PRESENT (so json_extract returns NULL, not
        # "field not found"), but its value must be JSON null.
        assert "il_usd" in payload
        assert payload["il_usd"] is None
        assert "hodl_value_usd" in payload
        assert payload["hodl_value_usd"] is None

    def test_il_usd_excludes_fee_income(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Codex review on PR #2259 (2026-05-13): ``cost_basis_usd`` is built
        from ``amount*_collected`` which carries principal + fees per
        ``LPCloseData`` semantics. Without subtracting ``fees_total_usd``
        first, fee income gets silently aliased into IL — an LP that
        opened 100 USDC + 0.05 WETH and closed with $10 fee income at
        prices unchanged from entry emits ``il_usd = +10`` against a
        true IL of 0.
        """
        self._patch_token_resolver(monkeypatch, {"USDC": 6, "WETH": 18})
        # Entry: 100 USDC ($1) + 0.05 WETH ($2000) = $200.
        prior_open = {
            "event_type": "LP_OPEN",
            "token0": "USDC",
            "token1": "WETH",
            "amount0": "100.0",
            "amount1": "0.05",
            "cost_basis_usd": "200.0",
        }
        # Close at identical prices. Collected: 100 USDC principal + 10 USDC
        # fees on token0, 0.05 WETH principal + 0 WETH fees on token1.
        # cost_basis_usd (principal+fees) = 110*1 + 0.05*2000 = 210.
        # fees_total_usd = 10*1 + 0*2000 = 10.
        # principal-only V_lp = 210 − 10 = 200 = V_hodl. True IL = 0.
        outbox, ledger = self._close_inputs(
            prices={"USDC": "1.00", "WETH": "2000.00"},
            prior_open=prior_open,
            amount0_collected_raw="110000000",  # 110 USDC (100 principal + 10 fees)
            amount1_collected_raw="50000000000000000",  # 0.05 WETH principal
            fees0_raw="10000000",  # 10 USDC fees
            fees1_raw="0",
        )

        result = handle_lp(outbox, ledger, prior_open_payload=prior_open)

        assert result is not None
        # Sanity-check the building blocks: V_lp gross-of-fees = 210;
        # fees_total_usd = 10; V_hodl = 200.
        assert result.cost_basis_usd == Decimal("210.00")
        assert result.fees_total_usd == Decimal("10.00")
        assert result.hodl_value_usd == Decimal("200.00")
        # The bug under fix: il_usd MUST be 0 (true IL), NOT +10 (which is
        # what the pre-fix formula would have emitted by counting fee
        # income as positive IL gain).
        assert result.il_usd == Decimal("0"), (
            "il_usd is computed gross of fees — fee income is leaking into IL. "
            "See _compute_lp_impermanent_loss docstring (Codex review)."
        )

    def test_il_usd_none_when_fees_unmeasured(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex review on PR #2259 (2026-05-13): when ``fees_total_usd`` is
        ``None`` (parser emitted no measurable fees in USD) we CANNOT
        extract the principal-only V_lp from ``cost_basis_usd``. Per
        CLAUDE.md "Empty ≠ Zero" we MUST fail closed — return
        ``il_usd=None`` — rather than silently substitute ``fees=0``
        which would over-credit principal_only V_lp and produce a wrong
        IL number. ``hodl_value_usd`` is still emitted because it depends
        only on prior_open amounts + close-time prices (principal-side).

        Production trigger for ``fees_total_usd is None``: an LP_CLOSE
        without typed ``lp_close_data`` falls through ``_resolve_lp_amounts``
        to the ledger-string fallback at ``lp_handler.py:179`` where
        ``fees0`` / ``fees1`` come back as ``None``. The next call to
        ``compute_lp_cost_basis(None, None, ...)`` then returns ``None``
        per the "both legs None" branch in ``lp_accounting.py:230``.
        """
        prior_open = {
            "event_type": "LP_OPEN",
            "token0": "USDC",
            "token1": "WETH",
            "amount0": "100.0",
            "amount1": "0.05",
            "cost_basis_usd": "200.0",
        }
        # Build a close ledger row WITHOUT typed lp_close_data so the
        # handler hits the fallback path that returns fees0=fees1=None.
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(
            led_id,
            intent_type="LP_CLOSE",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        ledger = _make_ledger_row(
            led_id,
            intent_type="LP_CLOSE",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            amount_in="120.0",  # cost_basis_usd path: 120*1 + 0.04*3000 = 240
            amount_out="0.04",
            price_inputs_json=json.dumps({"USDC": "1.00", "WETH": "3000.00"}),
            extracted_data_json="",  # no typed lp_close_data ⇒ fees come back None
        )

        result = handle_lp(outbox, ledger, prior_open_payload=prior_open)

        assert result is not None
        # cost_basis_usd is computable via the fallback (ledger strings + oracle).
        assert result.cost_basis_usd == Decimal("240.00")
        # But fees are unmeasured ⇒ fees_total_usd is None.
        assert result.fees_total_usd is None
        # Empty ≠ Zero: il_usd MUST be None (we cannot back out the fee
        # portion). Pre-fix this branch would have emitted il_usd = -10
        # (treating fees as zero), which is wrong.
        assert result.il_usd is None, (
            "il_usd MUST fail closed when fees_total_usd is None — "
            "fabricating fees=0 violates Empty ≠ Zero (Codex review)."
        )
        # hodl_value_usd is principal-side and stays measurable.
        assert result.hodl_value_usd == Decimal("250.00")

    def test_il_usd_none_on_lp_collect_fees(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex P1 on PR #2259 (2026-05-13): ``LP_COLLECT_FEES`` leaves the
        principal on-chain — ``amount*_collected`` carries fees only with zero
        principal, so ``cost_basis_usd ≈ fees_total_usd`` and the principal-
        only V_lp collapses to ~0. Without this guard the helper would emit
        ``il_usd = -V_hodl`` (a large bogus negative) into accounting on
        every fee collection, even though no IL has crystallised. IL is
        realised when principal is unwound; LP_COLLECT_FEES does not unwind.
        """
        self._patch_token_resolver(monkeypatch, {"USDC": 6, "WETH": 18})
        # Prior LP_OPEN: 100 USDC + 0.05 WETH ($200 at $1 / $2000).
        prior_open = {
            "event_type": "LP_OPEN",
            "token0": "USDC",
            "token1": "WETH",
            "amount0": "100.0",
            "amount1": "0.05",
            "cost_basis_usd": "200.0",
        }
        # Fee-collect event: amount*_collected = fees only (no principal
        # unwound). Production LP_COLLECT_FEES shape per VIB-3494.
        led_id = str(uuid.uuid4())
        outbox = _make_outbox_row(
            led_id,
            intent_type="LP_COLLECT_FEES",
            position_key="lp:uniswap_v3:arbitrum:0xwallet:0x1111111111111111111111111111111111111111",
            market_id="0x1111111111111111111111111111111111111111",
        )
        extracted = json.dumps({
            "lp_close_data": {
                "_type": "LPCloseData",
                "amount0_collected": "10000000",  # 10 USDC (= fees0)
                "amount1_collected": "5000000000000000",  # 0.005 WETH (= fees1)
                "fees0": "10000000",
                "fees1": "5000000000000000",
                "liquidity_removed": "0",
            }
        })
        ledger = _make_ledger_row(
            led_id,
            intent_type="LP_COLLECT_FEES",
            protocol="uniswap_v3",
            chain="arbitrum",
            token_in="USDC",
            token_out="WETH",
            extracted_data_json=extracted,
            price_inputs_json=json.dumps({"USDC": "1.00", "WETH": "2000.00"}),
        )

        result = handle_lp(outbox, ledger, prior_open_payload=prior_open)

        assert result is not None
        # Pre-fix this would have emitted il_usd ≈ -200 (= 0 − V_hodl_200).
        # Post-fix LP_COLLECT_FEES is OUT OF SCOPE for IL emission.
        assert result.il_usd is None, (
            "LP_COLLECT_FEES must NOT emit il_usd — principal is still on-chain "
            "so IL is not realised. Pre-fix this leaked a bogus -V_hodl number."
        )
        assert result.hodl_value_usd is None, (
            "hodl_value_usd is reserved for events where the helper actually "
            "runs the IL pipeline (LP_CLOSE only)."
        )

    def test_il_usd_none_when_either_entry_amount_is_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Gemini review on PR #2259 (2026-05-13): :class:`LPOpenEventPayload`
        requires both ``amount0`` and ``amount1`` as ``Decimal`` (see
        ``payload_schemas.py:287``). A ``None`` on either leg is a parse
        failure, NOT a single-sided position (single-sided LP OPENs land as
        ``Decimal("0")``). Computing a partial V_hodl against a full
        ``cost_basis_usd`` would emit a misleading ``il_usd``. Fail closed.
        """
        self._patch_token_resolver(monkeypatch, {"USDC": 6, "WETH": 18})
        # amount1 unparseable ⇒ data integrity issue. Pre-fix the helper
        # would have computed hodl from amount0 alone (partial V_hodl) and
        # subtracted it from a full cost_basis_usd.
        prior_open = {
            "event_type": "LP_OPEN",
            "token0": "USDC",
            "token1": "WETH",
            "amount0": "100.0",
            "amount1": None,  # parse failure
            "cost_basis_usd": "200.0",
        }
        outbox, ledger = self._close_inputs(
            prices={"USDC": "1.00", "WETH": "2000.00"},
            prior_open=prior_open,
            amount0_collected_raw="100000000",  # 100 USDC
            amount1_collected_raw="50000000000000000",  # 0.05 WETH
            fees0_raw="0",
            fees1_raw="0",
        )

        result = handle_lp(outbox, ledger, prior_open_payload=prior_open)
        assert result is not None
        assert result.il_usd is None, (
            "Either-leg-None entry amount must fail closed — partial V_hodl "
            "against full cost_basis_usd produces a misleading IL."
        )
        assert result.hodl_value_usd is None
