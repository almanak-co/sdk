"""VIB-4968: Curve LP events are no longer dropped by the LP category handler.

Before VIB-4968 the Curve LP category handler (``lp_handler.handle_lp``) wrote
ZERO typed ``accounting_events`` for LP_OPEN / LP_CLOSE: the position-key tail
is a bare Curve label (``3pool`` / ``2pool`` / …) and the parser stamped no
``pool_address``, so ``_resolve_lp_pool_address`` rejected every candidate and
``handle_lp`` returned ``None`` → full event drop.

The fix makes ``CurveReceiptParser`` stamp the canonical 0x pool address on
both ``LPOpenData`` and ``LPCloseData`` (the on-chain Add/RemoveLiquidity event
emitter IS the pool contract). The handler's receipt-extraction priority
(``_resolve_lp_pool_address`` step 1) then accepts that 0x address and books
the event — WITHOUT touching the shared ``_clean_pool_address_candidate`` /
``_resolve_lp_pool_address`` seam.

This test pins that boundary: a fungible-LP-shaped ``LPOpenData`` /
``LPCloseData`` (no NFT id, no tick bracket) carrying a 20-byte 0x
``pool_address``, keyed by a bare Curve label, MUST produce an
``LPAccountingEvent`` whose ``pool_address`` is that 0x address.
"""

from __future__ import annotations

import json
import re

from almanak.framework.accounting.category_handlers.lp_handler import handle_lp
from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData
from almanak.framework.observability.ledger import serialize_extracted_data

CURVE_3POOL = "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7"
ADDRESS_REGEX = re.compile(r"^0x[0-9a-f]{40}$")
WALLET = "0x1234567890abcdef1234567890abcdef12345678"
# Bare Curve label tail — the exact shape that pre-VIB-4968 caused the drop.
POSITION_KEY = f"lp:curve:ethereum:{WALLET}:3pool"


def _outbox_row() -> dict:
    return {
        "outbox_id": "ob-1",
        "deployment_id": "d1",
        "cycle_id": "c1",
        "position_key": POSITION_KEY,
        "wallet_address": WALLET,
        # market_id is the bare label too — also rejected by the candidate
        # cleaner, proving the receipt-extracted 0x address is what books it.
        "market_id": "3pool",
    }


def _ledger_row(*, intent_type: str, tx_hash: str, extracted_data_json: str) -> dict:
    return {
        "id": "le-1",
        "deployment_id": "d1",
        "cycle_id": "c1",
        "intent_type": intent_type,
        "protocol": "curve",
        "chain": "ethereum",
        "execution_mode": "paper",
        "tx_hash": tx_hash,
        # Curve LP intents carry no token symbols on the ledger row.
        "token_in": "",
        "token_out": "",
        "amount_in": "",
        "amount_out": "",
        "timestamp": "2026-06-04T00:00:00+00:00",
        "extracted_data_json": extracted_data_json,
        "price_inputs_json": "{}",
    }


class TestCurveLPOpenNoLongerDropped:
    def test_lp_open_books_event_with_canonical_pool_address(self):
        lp_open = LPOpenData(
            position_id=0,  # fungible LP: no NFT id
            amount0=50_000_000_000_000_000_000,
            amount1=50_000_000,
            tick_lower=None,
            tick_upper=None,
            liquidity=None,
            current_tick=None,
            pool_address=CURVE_3POOL,
            position_hash=None,
        )
        ledger = _ledger_row(
            intent_type="LP_OPEN",
            tx_hash="0xcurveopen",
            extracted_data_json=serialize_extracted_data({"lp_open_data": lp_open}),
        )

        event = handle_lp(_outbox_row(), ledger)

        assert event is not None, "VIB-4968: Curve LP_OPEN must no longer be dropped"
        assert event.pool_address == CURVE_3POOL
        assert ADDRESS_REGEX.fullmatch(event.pool_address) is not None
        # Fungible-LP directional null-contract.
        assert event.position_id is None  # position_id=0 -> "no discriminator"
        assert event.tick_lower is None
        assert event.tick_upper is None
        assert event.liquidity is None

        payload = json.loads(event.to_payload_json())
        assert payload["pool_address"] == CURVE_3POOL


class TestCurveLPCloseNoLongerDropped:
    def test_lp_close_books_event_with_canonical_pool_address(self):
        lp_close = LPCloseData(
            amount0_collected=33_000_000_000_000_000_000,
            amount1_collected=33_000_000,
            fees0=None,
            fees1=None,
            liquidity_removed=None,
            pool_address=CURVE_3POOL,
        )
        ledger = _ledger_row(
            intent_type="LP_CLOSE",
            tx_hash="0xcurveclose",
            extracted_data_json=serialize_extracted_data({"lp_close_data": lp_close}),
        )

        event = handle_lp(_outbox_row(), ledger)

        assert event is not None, "VIB-4968: Curve LP_CLOSE must no longer be dropped"
        assert event.pool_address == CURVE_3POOL
        assert ADDRESS_REGEX.fullmatch(event.pool_address) is not None

        payload = json.loads(event.to_payload_json())
        assert payload["pool_address"] == CURVE_3POOL
