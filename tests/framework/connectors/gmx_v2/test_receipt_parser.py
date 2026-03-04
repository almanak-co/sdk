"""Tests for GMX V2 Receipt Parser (Refactored).

Updated to use GMX V2 EventEmitter pattern:
- topic[0] = EventLog/EventLog1/EventLog2 signature (generic)
- topic[1] = keccak256(eventName) — the actual event identifier
"""

import pytest

from almanak.framework.connectors.gmx_v2.receipt_parser import (
    EVENT_TOPICS,
    GMXv2EventType,
    GMXv2ReceiptParser,
)

# Test data
GMX_ROUTER_ADDRESS = "0x7c68c7866a64fa2160f78eeae12217ffbf871fa8"
USER_ADDRESS = "0x742d35cc6634c0532925a3b844bc454e4438f44e"
MARKET_ADDRESS = "0x70d95587d40a2caf56bd97485ab3eec10bee6336"
USDC_ADDRESS = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
WETH_ADDRESS = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
POSITION_KEY = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

# EventLog1 signature (topic[0] for GMX V2 EventEmitter logs)
EVENT_LOG1_TOPIC = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc0a3ffa67d28dc3"


def create_position_increase_log(
    key,
    account,
    market,
    collateral_token,
    is_long,
    size_in_usd,
    size_in_tokens,
    collateral_amount,
    execution_price,
    size_delta_usd,
    collateral_delta_amount,
    index_token_price_max=0,
    index_token_price_min=0,
    collateral_token_price_max=0,
    collateral_token_price_min=0,
    price_impact_usd=0,
    order_type=0,
):
    """Create PositionIncrease log using EventEmitter pattern.

    Layout: account, market, collateral_token, is_long, size_in_usd,
    size_in_tokens, collateral_amount, execution_price, size_delta_usd,
    collateral_delta_amount, index_token_price_max, index_token_price_min,
    collateral_token_price_max, collateral_token_price_min, price_impact_usd,
    order_type, order_key
    """
    # GMX uses 10**30 for USD values and 10**18 for token values
    usd_scale = 10**30
    token_scale = 10**18

    data = "0x"
    data += f"{'00' * 12}{account[2:].lower()}"  # account
    data += f"{'00' * 12}{market[2:].lower()}"  # market
    data += f"{'00' * 12}{collateral_token[2:].lower()}"  # collateral_token
    data += f"{1 if is_long else 0:064x}"  # is_long
    data += f"{int(size_in_usd * usd_scale):064x}"  # size_in_usd
    data += f"{int(size_in_tokens * token_scale):064x}"  # size_in_tokens
    data += f"{int(collateral_amount * token_scale):064x}"  # collateral_amount
    data += f"{int(execution_price * usd_scale):064x}"  # execution_price
    data += f"{int(size_delta_usd * usd_scale):064x}"  # size_delta_usd
    data += f"{int(collateral_delta_amount * token_scale):064x}"  # collateral_delta_amount
    data += f"{int(index_token_price_max * usd_scale):064x}"  # index_token_price_max
    data += f"{int(index_token_price_min * usd_scale):064x}"  # index_token_price_min
    data += f"{int(collateral_token_price_max * usd_scale):064x}"  # collateral_token_price_max
    data += f"{int(collateral_token_price_min * usd_scale):064x}"  # collateral_token_price_min
    data += f"{int(price_impact_usd * usd_scale):064x}"  # price_impact_usd
    data += f"{order_type:064x}"  # order_type
    data += key[2:]  # order_key (bytes32)

    return {
        "address": GMX_ROUTER_ADDRESS,
        "topics": [
            EVENT_LOG1_TOPIC,  # topic[0] = EventLog1 signature
            EVENT_TOPICS["PositionIncrease"],  # topic[1] = keccak256("PositionIncrease")
            key,  # topic[2] = additional indexed param
        ],
        "data": data,
        "logIndex": 0,
    }


def create_position_decrease_log(
    key,
    account,
    market,
    collateral_token,
    is_long,
    size_in_usd,
    size_in_tokens,
    collateral_amount,
    execution_price,
    size_delta_usd,
    realized_pnl,
    collateral_delta_amount=0,
    index_token_price_max=0,
    index_token_price_min=0,
    collateral_token_price_max=0,
    collateral_token_price_min=0,
    price_impact_usd=0,
):
    """Create PositionDecrease log using EventEmitter pattern.

    Layout: account, market, collateral_token, is_long, size_in_usd,
    size_in_tokens, collateral_amount, execution_price, size_delta_usd,
    collateral_delta_amount, index_token_price_max, index_token_price_min,
    collateral_token_price_max, collateral_token_price_min, price_impact_usd,
    realized_pnl
    """
    usd_scale = 10**30
    token_scale = 10**18

    data = "0x"
    data += f"{'00' * 12}{account[2:].lower()}"
    data += f"{'00' * 12}{market[2:].lower()}"
    data += f"{'00' * 12}{collateral_token[2:].lower()}"
    data += f"{1 if is_long else 0:064x}"
    data += f"{int(size_in_usd * usd_scale):064x}"
    data += f"{int(size_in_tokens * token_scale):064x}"
    data += f"{int(collateral_amount * token_scale):064x}"
    data += f"{int(execution_price * usd_scale):064x}"
    data += f"{int(size_delta_usd * usd_scale):064x}"
    data += f"{int(collateral_delta_amount * token_scale):064x}"
    data += f"{int(index_token_price_max * usd_scale):064x}"
    data += f"{int(index_token_price_min * usd_scale):064x}"
    data += f"{int(collateral_token_price_max * usd_scale):064x}"
    data += f"{int(collateral_token_price_min * usd_scale):064x}"
    data += f"{int(price_impact_usd * usd_scale):064x}"
    data += f"{int(realized_pnl * usd_scale):064x}"

    return {
        "address": GMX_ROUTER_ADDRESS,
        "topics": [
            EVENT_LOG1_TOPIC,  # topic[0] = EventLog1 signature
            EVENT_TOPICS["PositionDecrease"],  # topic[1] = keccak256("PositionDecrease")
            key,  # topic[2] = additional indexed param
        ],
        "data": data,
        "logIndex": 1,
    }


def create_order_created_log(
    key,
    account,
    market,
    initial_collateral_token,
    order_type,
    is_long,
    size_delta_usd,
    initial_collateral_delta_amount,
    receiver=None,
    decrease_position_swap_type=0,
    trigger_price=0,
    acceptable_price=0,
    execution_fee=0,
    min_output_amount=0,
    updated_at_block=0,
):
    """Create OrderCreated log using EventEmitter pattern.

    Layout: account, receiver, market, initial_collateral_token, order_type,
    decrease_position_swap_type, is_long, size_delta_usd,
    initial_collateral_delta_amount, trigger_price, acceptable_price,
    execution_fee, min_output_amount, updated_at_block
    """
    usd_scale = 10**30
    token_scale = 10**18
    if receiver is None:
        receiver = account

    data = "0x"
    data += f"{'00' * 12}{account[2:].lower()}"  # account
    data += f"{'00' * 12}{receiver[2:].lower()}"  # receiver
    data += f"{'00' * 12}{market[2:].lower()}"  # market
    data += f"{'00' * 12}{initial_collateral_token[2:].lower()}"  # initial_collateral_token
    data += f"{order_type:064x}"  # order_type
    data += f"{decrease_position_swap_type:064x}"  # decrease_position_swap_type
    data += f"{1 if is_long else 0:064x}"  # is_long
    data += f"{int(size_delta_usd * usd_scale):064x}"  # size_delta_usd
    data += f"{int(initial_collateral_delta_amount * token_scale):064x}"  # initial_collateral_delta_amount
    data += f"{int(trigger_price * usd_scale):064x}"  # trigger_price
    data += f"{int(acceptable_price * usd_scale):064x}"  # acceptable_price
    data += f"{execution_fee:064x}"  # execution_fee (in wei)
    data += f"{int(min_output_amount * token_scale):064x}"  # min_output_amount
    data += f"{updated_at_block:064x}"  # updated_at_block

    return {
        "address": GMX_ROUTER_ADDRESS,
        "topics": [
            EVENT_LOG1_TOPIC,  # topic[0] = EventLog1 signature
            EVENT_TOPICS["OrderCreated"],  # topic[1] = keccak256("OrderCreated")
            key,  # topic[2] = additional indexed param
        ],
        "data": data,
        "logIndex": 2,
    }


def create_receipt(logs, status=1):
    """Create receipt with logs."""
    return {
        "transactionHash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "blockNumber": 12345678,
        "status": status,
        "logs": logs,
        "gasUsed": 200000,
    }


class TestGMXv2ReceiptParser:
    """Test GMX V2 receipt parser."""

    def test_parse_position_increase_event(self):
        """Test parsing PositionIncrease event."""
        parser = GMXv2ReceiptParser()

        log = create_position_increase_log(
            key=POSITION_KEY,
            account=USER_ADDRESS,
            market=MARKET_ADDRESS,
            collateral_token=USDC_ADDRESS,
            is_long=True,
            size_in_usd=50000.0,  # $50k position
            size_in_tokens=1.0,  # 1 ETH
            collateral_amount=10000.0,  # $10k collateral
            execution_price=50000.0,  # $50k per ETH
            size_delta_usd=50000.0,
            collateral_delta_amount=10000.0,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 1
        assert result.events[0].event_type == GMXv2EventType.POSITION_INCREASE
        assert result.events[0].event_name == "PositionIncrease"

        # Check parsed data
        assert len(result.position_increases) == 1
        pos = result.position_increases[0]
        assert pos.key == POSITION_KEY
        assert pos.account.lower() == USER_ADDRESS.lower()
        assert pos.market.lower() == MARKET_ADDRESS.lower()
        assert pos.collateral_token.lower() == USDC_ADDRESS.lower()
        assert pos.is_long
        assert float(pos.size_in_usd) == 50000.0
        assert float(pos.execution_price) == 50000.0

    def test_parse_position_decrease_event(self):
        """Test parsing PositionDecrease event."""
        parser = GMXv2ReceiptParser()

        log = create_position_decrease_log(
            key=POSITION_KEY,
            account=USER_ADDRESS,
            market=MARKET_ADDRESS,
            collateral_token=USDC_ADDRESS,
            is_long=True,
            size_in_usd=25000.0,  # Remaining position
            size_in_tokens=0.5,
            collateral_amount=5000.0,
            execution_price=52000.0,  # Exit price
            size_delta_usd=25000.0,  # Closing half
            realized_pnl=1000.0,  # $1k profit
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 1
        assert result.events[0].event_type == GMXv2EventType.POSITION_DECREASE

        # Check parsed data
        assert len(result.position_decreases) == 1
        pos = result.position_decreases[0]
        assert pos.key == POSITION_KEY
        assert pos.account.lower() == USER_ADDRESS.lower()
        assert pos.is_long
        assert float(pos.realized_pnl) == pytest.approx(1000.0, rel=1e-6)
        assert float(pos.execution_price) == pytest.approx(52000.0, rel=1e-6)

    def test_parse_order_created_event(self):
        """Test parsing OrderCreated event."""
        parser = GMXv2ReceiptParser()

        log = create_order_created_log(
            key=POSITION_KEY,
            account=USER_ADDRESS,
            market=MARKET_ADDRESS,
            initial_collateral_token=USDC_ADDRESS,
            order_type=0,  # Market order
            is_long=True,
            size_delta_usd=10000.0,
            initial_collateral_delta_amount=2000.0,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 1
        assert result.events[0].event_type == GMXv2EventType.ORDER_CREATED

        # Check parsed data
        data = result.events[0].data
        assert data["account"].lower() == USER_ADDRESS.lower()
        assert data["market"].lower() == MARKET_ADDRESS.lower()
        assert data["is_long"]
        assert float(data["size_delta_usd"]) == 10000.0

    def test_parse_short_position(self):
        """Test parsing short position increase."""
        parser = GMXv2ReceiptParser()

        log = create_position_increase_log(
            key=POSITION_KEY,
            account=USER_ADDRESS,
            market=MARKET_ADDRESS,
            collateral_token=USDC_ADDRESS,
            is_long=False,  # Short position
            size_in_usd=30000.0,
            size_in_tokens=1.0,
            collateral_amount=6000.0,
            execution_price=30000.0,
            size_delta_usd=30000.0,
            collateral_delta_amount=6000.0,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        pos = result.position_increases[0]
        assert not pos.is_long  # Verify short position

    def test_parse_multiple_events(self):
        """Test parsing multiple events in one receipt."""
        parser = GMXv2ReceiptParser()

        logs = [
            create_order_created_log(
                key=POSITION_KEY,
                account=USER_ADDRESS,
                market=MARKET_ADDRESS,
                initial_collateral_token=USDC_ADDRESS,
                order_type=0,
                is_long=True,
                size_delta_usd=10000.0,
                initial_collateral_delta_amount=2000.0,
            ),
            create_position_increase_log(
                key=POSITION_KEY,
                account=USER_ADDRESS,
                market=MARKET_ADDRESS,
                collateral_token=USDC_ADDRESS,
                is_long=True,
                size_in_usd=10000.0,
                size_in_tokens=0.2,
                collateral_amount=2000.0,
                execution_price=50000.0,
                size_delta_usd=10000.0,
                collateral_delta_amount=2000.0,
            ),
        ]
        receipt = create_receipt(logs)

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 2
        assert result.events[0].event_type == GMXv2EventType.ORDER_CREATED
        assert result.events[1].event_type == GMXv2EventType.POSITION_INCREASE

    def test_parse_empty_receipt(self):
        """Test parsing receipt with no logs."""
        parser = GMXv2ReceiptParser()

        receipt = create_receipt([])
        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 0
        assert len(result.position_increases) == 0

    def test_parse_unknown_event(self):
        """Test parsing receipt with unknown event."""
        parser = GMXv2ReceiptParser()

        unknown_log = {
            "address": GMX_ROUTER_ADDRESS,
            "topics": [
                "0x0000000000000000000000000000000000000000000000000000000000000000",
            ],
            "data": "0x",
            "logIndex": 0,
        }
        receipt = create_receipt([unknown_log])

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 0  # Unknown events are skipped

    def test_is_gmx_event(self):
        """Test is_gmx_event method with real event topic hashes."""
        parser = GMXv2ReceiptParser()

        # Known events (use real keccak256 hashes from EVENT_TOPICS)
        assert parser.is_gmx_event(EVENT_TOPICS["PositionIncrease"])
        assert parser.is_gmx_event(EVENT_TOPICS["PositionDecrease"])
        assert parser.is_gmx_event(EVENT_TOPICS["OrderCreated"])

        # Unknown event
        assert not parser.is_gmx_event("0x0000000000000000000000000000000000000000000000000000000000000000")

    def test_get_event_type(self):
        """Test get_event_type method with real event topic hashes."""
        parser = GMXv2ReceiptParser()

        # Known events (use real keccak256 hashes from EVENT_TOPICS)
        assert parser.get_event_type(EVENT_TOPICS["PositionIncrease"]) == GMXv2EventType.POSITION_INCREASE
        assert parser.get_event_type(EVENT_TOPICS["PositionDecrease"]) == GMXv2EventType.POSITION_DECREASE
        assert parser.get_event_type(EVENT_TOPICS["OrderCreated"]) == GMXv2EventType.ORDER_CREATED

        # Unknown event
        assert (
            parser.get_event_type("0x0000000000000000000000000000000000000000000000000000000000000000")
            == GMXv2EventType.UNKNOWN
        )

    def test_large_usd_values(self):
        """Test parsing with large USD values (10**30 scale)."""
        parser = GMXv2ReceiptParser()

        log = create_position_increase_log(
            key=POSITION_KEY,
            account=USER_ADDRESS,
            market=MARKET_ADDRESS,
            collateral_token=USDC_ADDRESS,
            is_long=True,
            size_in_usd=1000000.0,  # $1M position
            size_in_tokens=20.0,
            collateral_amount=200000.0,
            execution_price=50000.0,
            size_delta_usd=1000000.0,
            collateral_delta_amount=200000.0,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        pos = result.position_increases[0]
        assert float(pos.size_in_usd) == pytest.approx(1000000.0, rel=1e-6)
        assert float(pos.collateral_amount) == pytest.approx(200000.0, rel=1e-6)

    def test_position_increase_price_fields(self):
        """Test that collateral_token_price_max and index_token_price_max are decoded."""
        parser = GMXv2ReceiptParser()

        log = create_position_increase_log(
            key=POSITION_KEY,
            account=USER_ADDRESS,
            market=MARKET_ADDRESS,
            collateral_token=USDC_ADDRESS,
            is_long=True,
            size_in_usd=50000.0,
            size_in_tokens=1.0,
            collateral_amount=10000.0,
            execution_price=50000.0,
            size_delta_usd=50000.0,
            collateral_delta_amount=10000.0,
            index_token_price_max=50100.0,
            index_token_price_min=49900.0,
            collateral_token_price_max=1.001,
            collateral_token_price_min=0.999,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        pos = result.position_increases[0]
        assert float(pos.index_token_price_max) == pytest.approx(50100.0, rel=1e-6)
        assert float(pos.index_token_price_min) == pytest.approx(49900.0, rel=1e-6)
        assert float(pos.collateral_token_price_max) == pytest.approx(1.001, rel=1e-6)
        assert float(pos.collateral_token_price_min) == pytest.approx(0.999, rel=1e-6)

    def test_extract_leverage_with_price_data(self):
        """Test that extract_leverage works when price fields are decoded."""
        parser = GMXv2ReceiptParser()

        # 5x leverage: $50k position on $10k collateral at $1/token price
        log = create_position_increase_log(
            key=POSITION_KEY,
            account=USER_ADDRESS,
            market=MARKET_ADDRESS,
            collateral_token=USDC_ADDRESS,
            is_long=True,
            size_in_usd=50000.0,
            size_in_tokens=1.0,
            collateral_amount=10000.0,
            execution_price=50000.0,
            size_delta_usd=50000.0,
            collateral_delta_amount=10000.0,
            collateral_token_price_max=1.0,
        )
        receipt = create_receipt([log])

        leverage = parser.extract_leverage(receipt)
        assert leverage is not None
        assert float(leverage) == pytest.approx(5.0, rel=1e-6)

    def test_extract_execution_fee(self):
        """Test that execution_fee is decoded from order events."""
        parser = GMXv2ReceiptParser()

        fee_wei = 500000000000000  # 0.0005 ETH in wei
        log = create_order_created_log(
            key=POSITION_KEY,
            account=USER_ADDRESS,
            market=MARKET_ADDRESS,
            initial_collateral_token=USDC_ADDRESS,
            order_type=0,
            is_long=True,
            size_delta_usd=10000.0,
            initial_collateral_delta_amount=2000.0,
            execution_fee=fee_wei,
        )
        receipt = create_receipt([log])

        fee = parser.extract_fees_paid(receipt)
        assert fee == fee_wei

    def test_order_event_all_fields(self):
        """Test that all order event fields are decoded correctly."""
        parser = GMXv2ReceiptParser()

        log = create_order_created_log(
            key=POSITION_KEY,
            account=USER_ADDRESS,
            market=MARKET_ADDRESS,
            initial_collateral_token=USDC_ADDRESS,
            order_type=2,  # Limit order
            is_long=True,
            size_delta_usd=10000.0,
            initial_collateral_delta_amount=2000.0,
            receiver=USER_ADDRESS,
            trigger_price=48000.0,
            acceptable_price=47500.0,
            execution_fee=300000000000000,
            min_output_amount=1.5,
            updated_at_block=12345678,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)
        assert result.success
        assert len(result.order_events) == 1
        order = result.order_events[0]
        assert order.order_type == 2
        assert float(order.trigger_price) == pytest.approx(48000.0, rel=1e-6)
        assert float(order.acceptable_price) == pytest.approx(47500.0, rel=1e-6)
        assert order.execution_fee == 300000000000000
        assert float(order.min_output_amount) == pytest.approx(1.5, rel=1e-6)
        assert order.updated_at_block == 12345678

    def test_position_decrease_price_fields(self):
        """Test that price fields are decoded in PositionDecrease events."""
        parser = GMXv2ReceiptParser()

        log = create_position_decrease_log(
            key=POSITION_KEY,
            account=USER_ADDRESS,
            market=MARKET_ADDRESS,
            collateral_token=USDC_ADDRESS,
            is_long=True,
            size_in_usd=25000.0,
            size_in_tokens=0.5,
            collateral_amount=5000.0,
            execution_price=52000.0,
            size_delta_usd=25000.0,
            realized_pnl=1000.0,
            collateral_token_price_max=1.001,
            collateral_token_price_min=0.999,
            index_token_price_max=52100.0,
            index_token_price_min=51900.0,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)
        assert result.success
        pos = result.position_decreases[0]
        assert float(pos.collateral_token_price_max) == pytest.approx(1.001, rel=1e-6)
        assert float(pos.index_token_price_max) == pytest.approx(52100.0, rel=1e-6)
        assert float(pos.realized_pnl) == pytest.approx(1000.0, rel=1e-6)
