"""Unit tests for DriftReceiptParser."""

from almanak.framework.connectors.drift.receipt_parser import DriftReceiptParser


class TestDriftReceiptParser:
    def setup_method(self):
        self.parser = DriftReceiptParser()

    def test_parse_successful_receipt(self):
        receipt = {
            "meta": {
                "err": None,
                "preTokenBalances": [
                    {
                        "accountIndex": 3,
                        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "owner": "test-wallet",
                        "uiTokenAmount": {"uiAmount": 1000.0, "decimals": 6},
                    }
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 3,
                        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "owner": "test-wallet",
                        "uiTokenAmount": {"uiAmount": 900.0, "decimals": 6},
                    }
                ],
                "logMessages": [],
            }
        }
        result = self.parser.parse_receipt(receipt)
        assert result["success"] is True
        assert result["protocol"] == "drift"
        assert len(result["balance_changes"]) == 1
        assert result["balance_changes"][0]["delta"] == "-100.0"

    def test_parse_failed_receipt(self):
        receipt = {
            "meta": {
                "err": {"InstructionError": [0, "Custom(6001)"]},
                "logMessages": [],
            }
        }
        result = self.parser.parse_receipt(receipt)
        assert result["success"] is False
        assert "error" in result

    def test_parse_no_meta(self):
        receipt = {"meta": None}
        result = self.parser.parse_receipt(receipt)
        assert result["success"] is False

    def test_parse_with_fill_logs(self):
        receipt = {
            "meta": {
                "err": None,
                "preTokenBalances": [],
                "postTokenBalances": [],
                "logMessages": [
                    "Program log: Instruction: PlacePerpOrder",
                    "Program log: order_id=123, market_index=0, fill_price=150000000",
                ],
            }
        }
        result = self.parser.parse_receipt(receipt)
        assert result["success"] is True
        assert result["fill"] is not None
        assert result["fill"]["type"] == "perp_fill"
        assert "order_id" in result["fill"]["data"]

    def test_extract_perp_fill(self):
        receipt = {
            "meta": {
                "err": None,
                "logMessages": [
                    "Program log: fill order_id=42, base_amount=1000000000",
                ],
            }
        }
        fill = self.parser.extract_perp_fill(receipt)
        assert fill is not None
        assert fill["data"]["order_id"] == "42"

    def test_extract_perp_fill_no_fill(self):
        receipt = {
            "meta": {
                "err": None,
                "logMessages": ["Program log: no relevant data"],
            }
        }
        fill = self.parser.extract_perp_fill(receipt)
        assert fill is None

    # -----------------------------------------------------------------
    # Stub extraction methods (VIB-3204, VIB-3520)
    # -----------------------------------------------------------------

    def test_extract_protocol_fees_returns_none(self):
        """extract_protocol_fees is a no-op stub (VIB-3204) — always returns None."""
        assert self.parser.extract_protocol_fees({}) is None
        assert self.parser.extract_protocol_fees({"meta": {"err": None}}) is None

    def test_extract_funding_fee_usd_returns_none(self):
        """extract_funding_fee_usd is a no-op stub (VIB-3520) — always returns None."""
        assert self.parser.extract_funding_fee_usd({}) is None
        assert self.parser.extract_funding_fee_usd({"meta": {"err": None}}) is None

    def test_extract_position_id_returns_none(self):
        assert self.parser.extract_position_id({}) is None

    def test_extract_size_delta_returns_none(self):
        assert self.parser.extract_size_delta({}) is None

    def test_extract_collateral_returns_none(self):
        assert self.parser.extract_collateral({}) is None

    def test_extract_entry_price_returns_none(self):
        assert self.parser.extract_entry_price({}) is None

    def test_extract_leverage_returns_none(self):
        assert self.parser.extract_leverage({}) is None

    def test_extract_exit_price_returns_none(self):
        assert self.parser.extract_exit_price({}) is None

    def test_extract_realized_pnl_returns_none(self):
        assert self.parser.extract_realized_pnl({}) is None

    def test_extract_fees_paid_returns_none(self):
        assert self.parser.extract_fees_paid({}) is None

    def test_extract_collateral_returned_returns_none(self):
        assert self.parser.extract_collateral_returned({}) is None

    def test_supported_extractions_contains_all_perp_fields(self):
        """All PERP_OPEN and PERP_CLOSE fields must be declared so ResultEnricher
        doesn't emit warnings for any of them (VIB-3520 regression guard)."""
        required = {
            "position_id",
            "size_delta",
            "collateral",
            "entry_price",
            "leverage",
            "exit_price",
            "realized_pnl",
            "fees_paid",
            "collateral_returned",
            "protocol_fees",
            "funding_fee_usd",
        }
        assert required <= self.parser.SUPPORTED_EXTRACTIONS

    def test_balance_changes_empty_when_no_change(self):
        receipt = {
            "meta": {
                "err": None,
                "preTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": "test-mint",
                        "owner": "test-owner",
                        "uiTokenAmount": {"uiAmount": 100.0, "decimals": 6},
                    }
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 0,
                        "mint": "test-mint",
                        "owner": "test-owner",
                        "uiTokenAmount": {"uiAmount": 100.0, "decimals": 6},
                    }
                ],
                "logMessages": [],
            }
        }
        result = self.parser.parse_receipt(receipt)
        assert result["success"] is True
        assert len(result.get("balance_changes", [])) == 0
