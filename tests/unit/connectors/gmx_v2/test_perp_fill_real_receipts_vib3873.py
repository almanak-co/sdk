"""VIB-3873 real-receipt regression pins for GMX keyed fill-economics decode.

The fixture ``fixtures/gmx_keeper_receipts_arbitrum.json`` holds NON-EMPTY real
Arbitrum-mainnet GMX EventEmitter logs (PositionIncrease / PositionDecrease /
PositionFeesCollected / OrderExecuted), captured from public keeper transactions
(provenance recorded in the fixture's ``_provenance`` block). These bytes are the
real dynamic keyed EventUtils payload production emits — the exact shape the
legacy fixed-offset decode misreads (VIB-3873). Pinning the decoded economics
against them proves the keyed decode works on real chain bytes, not just on
synthetic encodings.

The close receipt is deliberately a SHORT position that closed at a LOSS, so it
exercises signed/negative fields (``realized_pnl_usd < 0``).
"""

import json
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.receipt_parser import GMXv2ReceiptParser

_FIXTURE = Path(__file__).parent / "fixtures" / "gmx_keeper_receipts_arbitrum.json"
_USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"


@pytest.fixture(scope="module")
def receipts() -> dict:
    return json.loads(_FIXTURE.read_text())


def _approx(value: Decimal | None, expected: str, tol: str = "0.01") -> bool:
    assert value is not None
    return abs(value - Decimal(expected)) <= Decimal(tol)


class TestRealOpenReceipt:
    def test_open_fill_decodes_measured_economics(self, receipts: dict) -> None:
        fill = GMXv2ReceiptParser().extract_perp_fill(receipts["open"])
        assert fill is not None
        assert fill.is_open is True
        assert fill.is_long is True
        assert fill.collateral_token == _USDC_ARBITRUM
        assert fill.position_key and fill.position_key.startswith("0x") and len(fill.position_key) == 66
        assert fill.order_key and len(fill.order_key) == 66
        # Real measured USD economics (sensible magnitudes for a ~$2.4k position).
        assert _approx(fill.size_delta_usd, "2422.85", "0.01")
        assert _approx(fill.position_fee_usd, "1.4537", "0.001")
        assert fill.collateral_delta_amount == Decimal("48504227")  # 48.5 USDC (6dp)
        # Price impact is SIGNED and negative here.
        assert fill.price_impact_usd is not None and fill.price_impact_usd < 0
        # Opens carry entry (native GMX price ratio), never exit / realized pnl.
        assert fill.entry_price is not None and fill.entry_price > 0
        assert fill.exit_price is None
        assert fill.realized_pnl_usd is None
        # Fresh open: measured-zero funding/borrowing (Decimal 0, not None).
        assert fill.funding_fee_usd == Decimal("0")
        assert fill.borrowing_fee_usd == Decimal("0")
        assert fill.keeper_tx_hash and fill.block_number and fill.block_number > 0


class TestRealCloseReceipt:
    def test_close_fill_decodes_signed_pnl_and_fees(self, receipts: dict) -> None:
        fill = GMXv2ReceiptParser().extract_perp_fill(receipts["close"])
        assert fill is not None
        assert fill.is_open is False
        assert fill.is_long is False
        assert fill.collateral_token == _USDC_ARBITRUM
        # Realized PnL is SIGNED and NEGATIVE (a real loss) — the signed-field pin.
        assert fill.realized_pnl_usd is not None
        assert fill.realized_pnl_usd < 0
        assert _approx(fill.realized_pnl_usd, "-30.96", "0.01")
        # Fees decoded to sensible USD via the event's own collateralTokenPrice.
        assert _approx(fill.funding_fee_usd, "2.4015", "0.001")
        assert _approx(fill.position_fee_usd, "2.8735", "0.001")
        assert _approx(fill.borrowing_fee_usd, "3.0381", "0.001")
        assert _approx(fill.size_delta_usd, "7183.75", "0.01")
        # Closes carry exit, never entry.
        assert fill.exit_price is not None and fill.exit_price > 0
        assert fill.entry_price is None

    def test_extract_funding_fee_usd_matches_fill(self, receipts: dict) -> None:
        parser = GMXv2ReceiptParser()
        funding = parser.extract_funding_fee_usd(receipts["close"])
        fill = parser.extract_perp_fill(receipts["close"])
        assert funding is not None
        assert funding == fill.funding_fee_usd
        assert _approx(funding, "2.4015", "0.001")


class TestRealReceiptProvenance:
    def test_fixture_records_provenance(self, receipts: dict) -> None:
        prov = receipts["_provenance"]
        assert "arbitrum" in prov["source"].lower()
        assert prov["open_tx"] and prov["close_tx"]

    def test_receipts_carry_all_four_event_types(self, receipts: dict) -> None:
        from almanak.connectors.gmx_v2.receipt_parser import TOPIC_TO_EVENT

        names = set()
        for key in ("open", "close"):
            for log in receipts[key]["logs"]:
                names.add(TOPIC_TO_EVENT.get(log["topics"][1].lower()))
        assert {"PositionIncrease", "PositionDecrease", "PositionFeesCollected", "OrderExecuted"} <= names
