"""VIB-4477 (T08): handler routes V4 LP events end-to-end with 32-byte pool_id.

Verifies the LP category handler (``lp_handler.handle_lp``) accepts the
V4-shaped ``LPOpenData`` / ``LPCloseData`` typed objects (pool_address as
32-byte ``pool_id`` hash, ``source="modify_liquidity"``) and emits an
``LPAccountingEvent`` whose ``pool_address`` survives untouched into the
payload-JSON output.

This is the integration boundary between T05 (V4 LP_OPEN parser),
T07 (V4 LP_CLOSE parser), and the rest of the accounting pipeline
(``AccountingWriter`` -> ``accounting_events``).
"""

from __future__ import annotations

import json
import re

import pytest

from almanak.framework.accounting.category_handlers.lp_handler import handle_lp
from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData

POOL_ID_32_BYTE = "0x" + "be" * 32
POOL_ID_REGEX = re.compile(r"^0x[0-9a-f]{64}$")
V3_POOL_ADDR_20_BYTE = "0x" + "aa" * 20
V3_POOL_REGEX = re.compile(r"^0x[0-9a-f]{40}$")
WALLET = "0x1234567890abcdef1234567890abcdef12345678"


def _outbox_row(position_key: str) -> dict:
    return {
        "outbox_id": "ob-1",
        "deployment_id": "d1",
        "strategy_id": "s1",
        "cycle_id": "c1",
        "position_key": position_key,
        "wallet_address": WALLET,
    }


def _ledger_row_open(*, protocol: str, extracted_data: dict) -> dict:
    return {
        "id": "le-1",
        "deployment_id": "d1",
        "strategy_id": "s1",
        "cycle_id": "c1",
        "intent_type": "LP_OPEN",
        "protocol": protocol,
        "chain": "arbitrum",
        "execution_mode": "paper",
        "tx_hash": "0xv4open",
        "token_in": "WETH",
        "token_out": "USDC",
        "amount_in": "",
        "amount_out": "",
        "timestamp": "2026-05-16T00:00:00+00:00",
        "extracted_data_json": json.dumps(extracted_data),
        "price_inputs_json": "{}",
    }


def _ledger_row_close(*, protocol: str, extracted_data: dict) -> dict:
    return {
        "id": "le-2",
        "deployment_id": "d1",
        "strategy_id": "s1",
        "cycle_id": "c1",
        "intent_type": "LP_CLOSE",
        "protocol": protocol,
        "chain": "arbitrum",
        "execution_mode": "paper",
        "tx_hash": "0xv4close",
        "token_in": "",
        "token_out": "",
        "amount_in": "",
        "amount_out": "",
        "timestamp": "2026-05-16T00:00:00+00:00",
        "extracted_data_json": json.dumps(extracted_data),
        "price_inputs_json": "{}",
    }


# =============================================================================
# 1. LP_OPEN: V4 32-byte pool_id flows through to the event
# =============================================================================


class TestV4LPOpenIntegration:
    def test_lp_open_event_carries_v4_pool_id(self):
        """LPOpenData with V4 32-byte pool_address -> LPAccountingEvent
        with the same 32-byte ``pool_address``."""
        lp_open = LPOpenData(
            position_id=1,
            tick_lower=-60000,
            tick_upper=60000,
            liquidity=500_000,
            amount0=10**18,
            amount1=2_000_000_000,
            pool_address=POOL_ID_32_BYTE,
            position_hash="0x" + "00" * 32,
        )
        # The handler uses ``deserialize_extracted_data`` so we wrap the
        # typed object via the canonical ``serialize_extracted_data`` helper
        # — same path the ledger writer uses in production.
        from almanak.framework.observability.ledger import serialize_extracted_data

        ed_json = serialize_extracted_data({"lp_open_data": lp_open})

        ledger = {
            "id": "le-1",
            "deployment_id": "d1",
            "strategy_id": "s1",
            "cycle_id": "c1",
            "intent_type": "LP_OPEN",
            "protocol": "uniswap_v4",
            "chain": "arbitrum",
            "execution_mode": "paper",
            "tx_hash": "0xv4open",
            "token_in": "WETH",
            "token_out": "USDC",
            "amount_in": "",
            "amount_out": "",
            "timestamp": "2026-05-16T00:00:00+00:00",
            "extracted_data_json": ed_json,
            "price_inputs_json": "{}",
        }
        outbox = _outbox_row(position_key=f"lp:uniswap_v4:arbitrum:{WALLET}:{POOL_ID_32_BYTE}")

        event = handle_lp(outbox, ledger)
        assert event is not None
        assert event.pool_address == POOL_ID_32_BYTE
        assert POOL_ID_REGEX.fullmatch(event.pool_address) is not None

        # The payload JSON serialised version must also carry the 32-byte form.
        payload = json.loads(event.to_payload_json())
        assert payload["pool_address"] == POOL_ID_32_BYTE
        assert POOL_ID_REGEX.fullmatch(payload["pool_address"]) is not None


# =============================================================================
# 2. LP_CLOSE: V4 32-byte pool_id flows through to the event
# =============================================================================


class TestV4LPCloseIntegration:
    def test_lp_close_event_carries_v4_pool_id(self):
        """LPCloseData with V4 32-byte pool_address + source="modify_liquidity"
        -> LPAccountingEvent with the same 32-byte ``pool_address``."""
        lp_close = LPCloseData(
            amount0_collected=10**18,
            amount1_collected=2_000_000_000,
            fees0=None,
            fees1=None,
            liquidity_removed=500_000,
            pool_address=POOL_ID_32_BYTE,
            source="modify_liquidity",
        )
        from almanak.framework.observability.ledger import serialize_extracted_data

        ed_json = serialize_extracted_data({"lp_close_data": lp_close})

        ledger = _ledger_row_close(
            protocol="uniswap_v4",
            extracted_data={},
        )
        ledger["extracted_data_json"] = ed_json
        outbox = _outbox_row(position_key=f"lp:uniswap_v4:arbitrum:{WALLET}:{POOL_ID_32_BYTE}")

        # No prior_open_payload — we still expect the 32-byte address from
        # the receipt extraction path.
        event = handle_lp(outbox, ledger)
        assert event is not None
        assert event.pool_address == POOL_ID_32_BYTE
        assert POOL_ID_REGEX.fullmatch(event.pool_address) is not None


# =============================================================================
# 3. V3 regression mirror: 20-byte pool_address still routes correctly
# =============================================================================


class TestV3RegressionMirror:
    def test_v3_lp_open_keeps_20_byte_pool_address(self):
        lp_open = LPOpenData(
            position_id=42,
            tick_lower=-60000,
            tick_upper=60000,
            liquidity=500_000,
            amount0=10**18,
            amount1=2_000_000_000,
            pool_address=V3_POOL_ADDR_20_BYTE,
            position_hash=None,
        )
        from almanak.framework.observability.ledger import serialize_extracted_data

        ed_json = serialize_extracted_data({"lp_open_data": lp_open})

        ledger = _ledger_row_open(protocol="uniswap_v3", extracted_data={})
        ledger["extracted_data_json"] = ed_json
        outbox = _outbox_row(position_key=f"lp:uniswap_v3:arbitrum:{WALLET}:{V3_POOL_ADDR_20_BYTE}")

        event = handle_lp(outbox, ledger)
        assert event is not None
        assert event.pool_address == V3_POOL_ADDR_20_BYTE
        assert V3_POOL_REGEX.fullmatch(event.pool_address) is not None
        # V3 must NOT match the V4 32-byte regex.
        assert POOL_ID_REGEX.fullmatch(event.pool_address) is None


# =============================================================================
# 4. _clean_pool_address_candidate accepts both shapes
# =============================================================================


class TestPoolAddressCandidateAcceptsBothShapes:
    """T02 (VIB-4471) tightened the cleaner to accept exactly 20/32-byte hex.
    Confirm V0 hasn't regressed that behaviour."""

    def test_20_byte_address_accepted(self):
        from almanak.framework.accounting.category_handlers.lp_handler import (
            _clean_pool_address_candidate,
        )

        assert _clean_pool_address_candidate(V3_POOL_ADDR_20_BYTE) == V3_POOL_ADDR_20_BYTE

    def test_32_byte_pool_id_accepted(self):
        from almanak.framework.accounting.category_handlers.lp_handler import (
            _clean_pool_address_candidate,
        )

        assert _clean_pool_address_candidate(POOL_ID_32_BYTE) == POOL_ID_32_BYTE

    @pytest.mark.parametrize(
        "bad",
        [
            "0x" + "ab" * 30,  # 60 hex chars — neither shape
            "0x" + "ab" * 50,  # 100 hex chars — too long
            "0x" + "GG" * 32,  # non-hex
            "weth/usdc/500",  # V3 fee tier descriptor
        ],
    )
    def test_invalid_shapes_rejected(self, bad: str):
        from almanak.framework.accounting.category_handlers.lp_handler import (
            _clean_pool_address_candidate,
        )

        assert _clean_pool_address_candidate(bad) == ""
