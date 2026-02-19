"""Tests for Lagoon vault receipt parser."""

from almanak.framework.connectors.lagoon.receipt_parser import (
    EVENT_TOPICS,
    LagoonEventType,
    LagoonReceiptParser,
    NewTotalAssetsEventData,
    SettleDepositEventData,
    SettleRedeemEventData,
)


# Helper to build uint256 hex (64 hex chars, big-endian)
def _uint256_hex(value: int) -> str:
    return f"{value:064x}"


# Helper to build an indexed topic (0x-prefixed 32-byte hex)
def _indexed_topic(value: int) -> str:
    return "0x" + _uint256_hex(value)


# Helper to build log data with multiple uint256 values
def _encode_data(*values: int) -> str:
    return "0x" + "".join(_uint256_hex(v) for v in values)


class TestLagoonReceiptParserBasic:
    """Basic receipt parsing tests."""

    def test_parse_receipt_with_settle_deposit(self):
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xdeposit123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [
                        EVENT_TOPICS["SettleDeposit"],
                        _indexed_topic(1),  # epochId (indexed)
                        _indexed_topic(0),  # settledId (indexed)
                    ],
                    "data": _encode_data(
                        1000000,     # total_assets
                        500000,      # total_supply
                        200000,      # assets_deposited
                        100000,      # shares_minted
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.settle_deposits) == 1
        assert result.settle_deposits[0].epoch_id == 1
        assert result.settle_deposits[0].total_assets == 1000000
        assert result.settle_deposits[0].total_supply == 500000
        assert result.settle_deposits[0].assets_deposited == 200000
        assert result.settle_deposits[0].shares_minted == 100000
        assert result.transaction_hash == "0xdeposit123"
        assert result.block_number == 12345

    def test_parse_receipt_with_settle_redeem(self):
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xredeem456",
            "blockNumber": 67890,
            "status": 1,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [
                        EVENT_TOPICS["SettleRedeem"],
                        _indexed_topic(2),  # epochId (indexed)
                        _indexed_topic(1),  # settledId (indexed)
                    ],
                    "data": _encode_data(
                        800000,      # total_assets
                        400000,      # total_supply
                        150000,      # assets_withdrawn
                        75000,       # shares_burned
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.settle_redeems) == 1
        assert result.settle_redeems[0].epoch_id == 2
        assert result.settle_redeems[0].total_assets == 800000
        assert result.settle_redeems[0].total_supply == 400000
        assert result.settle_redeems[0].assets_withdrawn == 150000
        assert result.settle_redeems[0].shares_burned == 75000
        assert result.transaction_hash == "0xredeem456"

    def test_parse_receipt_with_new_total_assets(self):
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xupdate789",
            "blockNumber": 11111,
            "status": 1,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [EVENT_TOPICS["NewTotalAssetsUpdated"]],
                    "data": _encode_data(5000000),  # new_total_assets
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.new_total_assets_events) == 1
        assert result.new_total_assets_events[0].new_total_assets == 5000000

    def test_parse_receipt_empty_logs(self):
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xempty",
            "blockNumber": 123,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.settle_deposits) == 0
        assert len(result.settle_redeems) == 0
        assert len(result.new_total_assets_events) == 0

    def test_parse_receipt_no_logs_key(self):
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xnologs",
            "blockNumber": 456,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.settle_deposits) == 0

    def test_parse_receipt_unknown_event_ignored(self):
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xunknown",
            "blockNumber": 789,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": ["0x" + "ff" * 32],  # Unknown event topic
                    "data": "0x" + "00" * 32,
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.settle_deposits) == 0
        assert len(result.settle_redeems) == 0
        assert len(result.new_total_assets_events) == 0

    def test_parse_receipt_log_with_no_topics(self):
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xnotopics",
            "blockNumber": 100,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [],
                    "data": "0x",
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.settle_deposits) == 0


class TestLagoonReceiptParserMultipleEvents:
    """Tests for receipts with multiple events."""

    def test_parse_receipt_with_deposit_and_redeem(self):
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xmulti",
            "blockNumber": 22222,
            "status": 1,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [
                        EVENT_TOPICS["SettleDeposit"],
                        _indexed_topic(1),  # epochId
                        _indexed_topic(0),  # settledId
                    ],
                    "data": _encode_data(1000000, 500000, 200000, 100000),
                },
                {
                    "address": "0xvault",
                    "topics": [
                        EVENT_TOPICS["SettleRedeem"],
                        _indexed_topic(1),  # epochId
                        _indexed_topic(0),  # settledId
                    ],
                    "data": _encode_data(900000, 450000, 100000, 50000),
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.settle_deposits) == 1
        assert len(result.settle_redeems) == 1
        assert result.settle_deposits[0].assets_deposited == 200000
        assert result.settle_redeems[0].assets_withdrawn == 100000

    def test_parse_receipt_with_update_and_settle(self):
        """A typical full settlement cycle: update valuation then settle."""
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xfullcycle",
            "blockNumber": 33333,
            "status": 1,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [EVENT_TOPICS["NewTotalAssetsUpdated"]],
                    "data": _encode_data(2000000),
                },
                {
                    "address": "0xvault",
                    "topics": [
                        EVENT_TOPICS["SettleDeposit"],
                        _indexed_topic(3),  # epochId
                        _indexed_topic(2),  # settledId
                    ],
                    "data": _encode_data(2000000, 1000000, 500000, 250000),
                },
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.new_total_assets_events) == 1
        assert len(result.settle_deposits) == 1
        assert result.new_total_assets_events[0].new_total_assets == 2000000
        assert result.settle_deposits[0].epoch_id == 3


class TestLagoonReceiptParserBytesTopics:
    """Tests for topic handling with bytes input."""

    def test_bytes_topic(self):
        parser = LagoonReceiptParser(chain="ethereum")

        # Convert the hex topic to bytes
        topic_hex = EVENT_TOPICS["NewTotalAssetsUpdated"]
        topic_bytes = bytes.fromhex(topic_hex[2:])

        receipt = {
            "transactionHash": "0xbytes",
            "blockNumber": 100,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [topic_bytes],
                    "data": _encode_data(9999),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.new_total_assets_events) == 1
        assert result.new_total_assets_events[0].new_total_assets == 9999

    def test_bytes_transaction_hash(self):
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": bytes.fromhex("abcdef1234567890" * 4),
            "blockNumber": 100,
            "logs": [],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash.startswith("0x")

    def test_bytes_indexed_topics(self):
        """Test that indexed topics provided as bytes are decoded correctly."""
        parser = LagoonReceiptParser(chain="ethereum")

        epoch_id = 42
        settled_id = 10
        epoch_bytes = epoch_id.to_bytes(32, "big")
        settled_bytes = settled_id.to_bytes(32, "big")

        receipt = {
            "transactionHash": "0xbytesidx",
            "blockNumber": 200,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [
                        EVENT_TOPICS["SettleDeposit"],
                        epoch_bytes,
                        settled_bytes,
                    ],
                    "data": _encode_data(5000, 2500, 1000, 500),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.settle_deposits) == 1
        assert result.settle_deposits[0].epoch_id == 42
        assert result.settle_deposits[0].total_assets == 5000


class TestLagoonEventDataModels:
    """Tests for event data model methods."""

    def test_settle_deposit_to_dict(self):
        event = SettleDepositEventData(
            epoch_id=1,
            total_assets=1000000,
            total_supply=500000,
            assets_deposited=200000,
            shares_minted=100000,
        )

        d = event.to_dict()
        assert d["epoch_id"] == 1
        assert d["total_assets"] == 1000000
        assert d["total_supply"] == 500000
        assert d["assets_deposited"] == 200000
        assert d["shares_minted"] == 100000

    def test_settle_redeem_to_dict(self):
        event = SettleRedeemEventData(
            epoch_id=2,
            total_assets=800000,
            total_supply=400000,
            assets_withdrawn=150000,
            shares_burned=75000,
        )

        d = event.to_dict()
        assert d["epoch_id"] == 2
        assert d["assets_withdrawn"] == 150000
        assert d["shares_burned"] == 75000

    def test_new_total_assets_to_dict(self):
        event = NewTotalAssetsEventData(new_total_assets=5000000)

        d = event.to_dict()
        assert d["new_total_assets"] == 5000000


class TestLagoonParseResultModel:
    """Tests for the LagoonParseResult model."""

    def test_parse_result_to_dict(self):
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xtest",
            "blockNumber": 100,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [EVENT_TOPICS["NewTotalAssetsUpdated"]],
                    "data": _encode_data(42),
                }
            ],
        }

        result = parser.parse_receipt(receipt)
        d = result.to_dict()

        assert d["success"] is True
        assert d["transaction_hash"] == "0xtest"
        assert d["block_number"] == 100
        assert len(d["new_total_assets_events"]) == 1
        assert d["new_total_assets_events"][0]["new_total_assets"] == 42
        assert d["error"] is None

    def test_failed_parse_result(self):
        parser = LagoonReceiptParser(chain="ethereum")

        # Force a top-level error by providing non-iterable logs
        receipt = {
            "transactionHash": "0xerr",
            "blockNumber": 0,
            "logs": "not_a_list",
        }

        result = parser.parse_receipt(receipt)

        assert result.success is False
        assert result.error is not None


class TestLagoonSingleLogParsing:
    """Tests for single-log parse_event method."""

    def test_parse_event_settle_deposit(self):
        parser = LagoonReceiptParser(chain="ethereum")

        log = {
            "topics": [
                EVENT_TOPICS["SettleDeposit"],
                _indexed_topic(5),  # epochId
                _indexed_topic(4),  # settledId
            ],
            "data": _encode_data(3000000, 1500000, 600000, 300000),
        }

        event = parser.parse_event(log)

        assert isinstance(event, SettleDepositEventData)
        assert event.epoch_id == 5
        assert event.assets_deposited == 600000

    def test_parse_event_settle_redeem(self):
        parser = LagoonReceiptParser(chain="ethereum")

        log = {
            "topics": [
                EVENT_TOPICS["SettleRedeem"],
                _indexed_topic(6),  # epochId
                _indexed_topic(5),  # settledId
            ],
            "data": _encode_data(2500000, 1200000, 400000, 200000),
        }

        event = parser.parse_event(log)

        assert isinstance(event, SettleRedeemEventData)
        assert event.epoch_id == 6
        assert event.shares_burned == 200000

    def test_parse_event_new_total_assets(self):
        parser = LagoonReceiptParser(chain="ethereum")

        log = {
            "topics": [EVENT_TOPICS["NewTotalAssetsUpdated"]],
            "data": _encode_data(7777777),
        }

        event = parser.parse_event(log)

        assert isinstance(event, NewTotalAssetsEventData)
        assert event.new_total_assets == 7777777

    def test_parse_event_unknown_returns_none(self):
        parser = LagoonReceiptParser(chain="ethereum")

        log = {
            "topics": ["0x" + "aa" * 32],
            "data": "0x",
        }

        event = parser.parse_event(log)
        assert event is None

    def test_parse_event_no_topics_returns_none(self):
        parser = LagoonReceiptParser(chain="ethereum")

        log = {
            "topics": [],
            "data": "0x",
        }

        event = parser.parse_event(log)
        assert event is None


class TestLagoonReceiptParserRegistration:
    """Test that the parser is correctly registered in the receipt registry."""

    def test_lagoon_parser_registered(self):
        from almanak.framework.execution.receipt_registry import is_parser_available

        assert is_parser_available("lagoon") is True

    def test_lagoon_parser_can_be_loaded(self):
        from almanak.framework.execution.receipt_registry import get_parser

        parser = get_parser("lagoon")
        assert isinstance(parser, LagoonReceiptParser)

    def test_lagoon_parser_with_chain_kwarg(self):
        from almanak.framework.execution.receipt_registry import get_parser

        parser = get_parser("lagoon", chain="arbitrum")
        assert isinstance(parser, LagoonReceiptParser)
        assert parser.chain == "arbitrum"


class TestLagoonReceiptParserLargeValues:
    """Test with large uint256 values typical of on-chain data."""

    def test_large_total_assets(self):
        parser = LagoonReceiptParser(chain="ethereum")

        # 1 billion USDC (6 decimals) = 1_000_000 * 10^6
        large_assets = 1_000_000_000_000

        receipt = {
            "transactionHash": "0xlarge",
            "blockNumber": 44444,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [EVENT_TOPICS["NewTotalAssetsUpdated"]],
                    "data": _encode_data(large_assets),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.new_total_assets_events[0].new_total_assets == large_assets

    def test_max_uint256_epoch(self):
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xmaxepoch",
            "blockNumber": 55555,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [
                        EVENT_TOPICS["SettleDeposit"],
                        _indexed_topic(999999),  # epochId (indexed)
                        _indexed_topic(999998),  # settledId (indexed)
                    ],
                    "data": _encode_data(
                        10**18,       # total_assets (1 ETH worth)
                        5 * 10**17,   # total_supply
                        10**17,       # assets_deposited
                        5 * 10**16,   # shares_minted
                    ),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.settle_deposits[0].epoch_id == 999999
        assert result.settle_deposits[0].total_assets == 10**18

    def test_missing_indexed_topics_defaults_to_zero(self):
        """If indexed topics are missing, epoch_id defaults to 0."""
        parser = LagoonReceiptParser(chain="ethereum")

        receipt = {
            "transactionHash": "0xmissing",
            "blockNumber": 66666,
            "logs": [
                {
                    "address": "0xvault",
                    "topics": [EVENT_TOPICS["SettleDeposit"]],  # No indexed topics
                    "data": _encode_data(100, 50, 25, 10),
                }
            ],
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.settle_deposits) == 1
        assert result.settle_deposits[0].epoch_id == 0  # Defaults when topic missing
        assert result.settle_deposits[0].total_assets == 100
