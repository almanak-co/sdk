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
        # chain is required for VIB-6110 price scaling (executionPrice → USD-per-token).
        fill = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(receipts["open"])
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
        # Opens carry entry (USD-per-token, VIB-6110), never exit / realized pnl. This
        # fixture's open market is BTC/USD (index-token decimals 8), so executionPrice
        # scales to a real ~$64.5k BTC price — NOT the raw 6.45e-4 ratio the field
        # shipped before VIB-6110 (the 1.9e-15-class scaling bug).
        assert fill.entry_price is not None
        assert fill.entry_price > Decimal("1000")  # a real USD price, not the sub-1 raw ratio
        assert _approx(fill.entry_price, "64528.73", "0.5")
        assert fill.exit_price is None
        assert fill.realized_pnl_usd is None
        # Fresh open: measured-zero funding/borrowing (Decimal 0, not None).
        assert fill.funding_fee_usd == Decimal("0")
        assert fill.borrowing_fee_usd == Decimal("0")
        assert fill.keeper_tx_hash and fill.block_number and fill.block_number > 0


class TestRealCloseReceipt:
    def test_close_fill_decodes_signed_pnl_and_fees(self, receipts: dict) -> None:
        # chain provided so scaling is attempted — this fixture's close market is UNLISTED.
        fill = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(receipts["close"])
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
        # This fixture's close market (0xdab2…) is NOT in GMX_V2_INDEX_TOKEN_DECIMALS, so
        # VIB-6110 fails closed: exit_price is UNMEASURED (None), NEVER the raw GMX ratio.
        # (Close-side scaling on a LISTED market is pinned by
        # test_perp_fill_data_vib3873.TestPerpFillClose.test_close_fill_measures_exit_pnl_and_fees.)
        assert fill.exit_price is None
        assert fill.entry_price is None

    def test_extract_funding_fee_usd_matches_fill(self, receipts: dict) -> None:
        parser = GMXv2ReceiptParser()
        funding = parser.extract_funding_fee_usd(receipts["close"])
        fill = parser.extract_perp_fill(receipts["close"])
        assert funding is not None
        assert fill is not None
        assert funding == fill.funding_fee_usd
        assert _approx(funding, "2.4015", "0.001")


class TestSettlementPayloadPriceIsPlausibleUsd:
    """VIB-6110 end-to-end: the real open fill, built into a PERP_SETTLEMENT event via the
    framework builder (a pass-through), carries a plausible USD entry_price — NOT the
    1.9e-15-class raw ratio that degraded cost-basis to ESTIMATED on the mainnet re-proof."""

    @staticmethod
    def _event(receipt: dict, *, is_open: bool):
        from almanak.connectors._strategy_base.runner_hook_registry import (
            PerpSettlementState,
            PerpSettlementVerdict,
        )
        from almanak.framework.accounting.perp_settlement_accounting import build_perp_settlement_event

        fill = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(receipt)
        assert fill is not None
        verdict = PerpSettlementVerdict(
            order_key=fill.order_key or "0x",
            state=PerpSettlementState.EXECUTED,
            terminal=True,
            fill_data=fill,
            keeper_tx_hash=fill.keeper_tx_hash,
        )
        return build_perp_settlement_event(
            verdict=verdict,
            submission_ledger_entry_id="led-1",
            deployment_id="deployment:wi7",
            cycle_id="cyc-1",
            execution_mode="paper",
            chain="arbitrum",
            protocol="gmx_v2",
            wallet_address="0xwallet",
            is_open=is_open,
        )

    def test_open_settlement_payload_entry_price_is_real_usd(self, receipts: dict) -> None:
        # Assert the SERIALIZED numeric value (what persists), not just key-presence: the
        # payload always emits the key even when null/wrongly-scaled. Mutation-resistant —
        # reverting the scaling makes entry_price ~6.45e-4 and both asserts fail.
        payload = json.loads(self._event(receipts["open"], is_open=True).to_payload_json())
        entry = Decimal(str(payload["entry_price"]))  # persisted as a decimal string
        assert entry > Decimal("1000")  # a real BTC-market USD price, NOT the sub-1 raw ratio or 0
        assert _approx(entry, "64528.73", "0.5")
        assert payload["exit_price"] is None  # an open has no exit

    def test_close_settlement_payload_exit_price_null_on_unlisted_market(self, receipts: dict) -> None:
        # The close fixture's market is unlisted → exit_price serializes as null
        # (Empty≠Zero), NEVER the raw ratio or 0.
        payload = json.loads(self._event(receipts["close"], is_open=False).to_payload_json())
        assert payload["exit_price"] is None
        assert payload["entry_price"] is None


class TestRealBytesOrderCorrelation:
    """VIB-6110: pin that REAL GMX payloads carry a matching ``orderKey``.

    The batched-keeper correlation tests use synthetic ``abi_encode`` fixtures,
    which always include ``orderKey`` by construction. Nothing else in CI proves
    that real mainnet PositionIncrease / PositionDecrease / PositionFeesCollected
    payloads carry the key the settlement path correlates on. Without this pin, a
    GMX payload-key rename would make every settlement silently UNMEASURED while
    the whole suite stayed green — a correlation that is too STRICT fails just as
    quietly as one that is too loose.
    """

    @staticmethod
    def _order_key(receipt: dict) -> str:
        key = GMXv2ReceiptParser(chain="arbitrum").extract_perp_fill(receipt).order_key
        assert key, "real receipt carried no decodable order key"
        return key

    def test_real_open_correlates_on_its_own_order_key(self, receipts: dict) -> None:
        parser = GMXv2ReceiptParser(chain="arbitrum")
        key = self._order_key(receipts["open"])
        fill = parser.extract_perp_fill(receipts["open"], order_key=key)

        assert fill is not None, "real PositionIncrease did not match its own orderKey"
        assert fill.order_key == key
        assert fill.is_open is True
        assert _approx(fill.entry_price, "64528.73", "0.5")
        # Fees come from the real PositionFeesCollected — proving IT carries orderKey too.
        assert _approx(fill.position_fee_usd, "1.4537", "0.001")

    def test_real_close_correlates_on_its_own_order_key(self, receipts: dict) -> None:
        parser = GMXv2ReceiptParser(chain="arbitrum")
        key = self._order_key(receipts["close"])
        fill = parser.extract_perp_fill(receipts["close"], order_key=key)

        assert fill is not None, "real PositionDecrease did not match its own orderKey"
        assert fill.order_key == key
        assert fill.is_open is False
        assert fill.realized_pnl_usd is not None

    def test_real_receipt_rejects_a_foreign_order_key(self, receipts: dict) -> None:
        """A key not present in the receipt yields None, never the resident fill."""
        parser = GMXv2ReceiptParser(chain="arbitrum")
        assert parser.extract_perp_fill(receipts["open"], order_key="0x" + "07" * 32) is None

    def test_real_receipts_are_emitted_by_the_canonical_event_emitter(self, receipts: dict) -> None:
        """The correlation guard filters to this address — pin that real logs use it."""
        for key in ("open", "close"):
            emitters = {str(log["address"]).lower() for log in receipts[key]["logs"]}
            assert emitters == {"0xc8ee91a54287db53897056e12d9819156d3822fb"}


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
