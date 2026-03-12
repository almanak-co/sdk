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
