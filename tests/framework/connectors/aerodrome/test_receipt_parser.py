"""Tests for Aerodrome Receipt Parser (Refactored)."""

from almanak.framework.connectors.aerodrome.receipt_parser import (
    AerodromeEventType,
    AerodromeReceiptParser,
)

# Test data
POOL_ADDRESS = "0x6cDcb1C4A4D1C3C6d054b27AC5B77e89eAFb971d"
USER_ADDRESS = "0x742d35cc6634c0532925a3b844bc454e4438f44e"
WETH_ADDRESS = "0x4200000000000000000000000000000000000006"
USDC_ADDRESS = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


def create_swap_log(sender, to, amount0_in, amount1_in, amount0_out, amount1_out):
    data = f"0x{amount0_in:064x}{amount1_in:064x}{amount0_out:064x}{amount1_out:064x}"
    return {
        "address": POOL_ADDRESS,
        "topics": [
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
            f"0x000000000000000000000000{sender[2:].lower()}",
            f"0x000000000000000000000000{to[2:].lower()}",
        ],
        "data": data,
        "logIndex": 0,
    }


def create_mint_log(sender, amount0, amount1):
    data = f"0x{amount0:064x}{amount1:064x}"
    return {
        "address": POOL_ADDRESS,
        "topics": [
            "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f",
            f"0x000000000000000000000000{sender[2:].lower()}",
        ],
        "data": data,
        "logIndex": 1,
    }


def create_burn_log(sender, to, amount0, amount1):
    data = f"0x{amount0:064x}{amount1:064x}"
    return {
        "address": POOL_ADDRESS,
        "topics": [
            "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496",
            f"0x000000000000000000000000{sender[2:].lower()}",
            f"0x000000000000000000000000{to[2:].lower()}",
        ],
        "data": data,
        "logIndex": 2,
    }


class TestAerodromeReceiptParser:
    """Tests for AerodromeReceiptParser."""

    def test_parse_swap_token0_input(self):
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
            token0_symbol="USDC",
            token1_symbol="WETH",
            token0_decimals=6,
            token1_decimals=18,
        )

        amount_in = 1_000_000_000  # 1000 USDC
        amount_out = 500_000_000_000_000_000  # 0.5 WETH

        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [create_swap_log(USER_ADDRESS, USER_ADDRESS, amount_in, 0, 0, amount_out)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == AerodromeEventType.SWAP
        assert result.events[0].data["amount0_in"] == amount_in
        assert result.events[0].data["amount1_out"] == amount_out
        assert result.swap_events[0].token0_is_input is True
        assert result.swap_result.amount_in == amount_in
        assert result.swap_result.amount_out == amount_out

    def test_parse_swap_token1_input(self):
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
        )

        amount_in = 1_000_000_000_000_000_000  # 1 WETH
        amount_out = 2_000_000_000  # 2000 USDC

        receipt = {
            "transactionHash": "0x456",
            "blockNumber": 12346,
            "status": 1,
            "logs": [create_swap_log(USER_ADDRESS, USER_ADDRESS, 0, amount_in, amount_out, 0)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.swap_events[0].token1_is_input is True
        assert result.swap_result.token_in == WETH_ADDRESS.lower()
        assert result.swap_result.token_out == USDC_ADDRESS.lower()

    def test_parse_mint_event(self):
        parser = AerodromeReceiptParser()

        amount0 = 1_000_000
        amount1 = 2_000_000

        receipt = {
            "transactionHash": "0x789",
            "blockNumber": 12347,
            "status": 1,
            "logs": [create_mint_log(USER_ADDRESS, amount0, amount1)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.mint_events) == 1
        assert result.mint_events[0].amount0 == amount0
        assert result.mint_events[0].amount1 == amount1
        assert result.liquidity_result is not None
        assert result.liquidity_result.operation == "add"

    def test_parse_burn_event(self):
        parser = AerodromeReceiptParser()

        amount0 = 1_000_000
        amount1 = 2_000_000
        receiver = "0x1234567890123456789012345678901234567890"

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 12348,
            "status": 1,
            "logs": [create_burn_log(USER_ADDRESS, receiver, amount0, amount1)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.burn_events) == 1
        assert result.burn_events[0].amount0 == amount0
        assert result.burn_events[0].to == receiver.lower()
        assert result.liquidity_result is not None
        assert result.liquidity_result.operation == "remove"

    def test_slippage_calculation(self):
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
            token0_decimals=6,
            token1_decimals=18,
        )

        amount_in = 1_000_000_000
        amount_out_actual = 490_000_000_000_000_000
        amount_out_quoted = 500_000_000_000_000_000

        receipt = {
            "transactionHash": "0xdef",
            "blockNumber": 12349,
            "status": 1,
            "logs": [create_swap_log(USER_ADDRESS, USER_ADDRESS, amount_in, 0, 0, amount_out_actual)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt, quoted_amount_out=amount_out_quoted)

        assert result.swap_result.slippage_bps == 200  # 2%

    def test_token_symbol_resolution(self):
        parser = AerodromeReceiptParser(
            chain="base",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
        )

        assert parser.token0_symbol == "USDC"
        assert parser.token1_symbol == "WETH"
        assert parser.token0_decimals == 6
        assert parser.token1_decimals == 18

    def test_failed_transaction(self):
        parser = AerodromeReceiptParser()

        receipt = {
            "transactionHash": "0x111",
            "blockNumber": 12350,
            "status": 0,
            "logs": [create_swap_log(USER_ADDRESS, USER_ADDRESS, 1000000, 0, 0, 500000)],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_success is False
        assert result.error == "Transaction reverted"

    def test_empty_logs(self):
        parser = AerodromeReceiptParser()

        receipt = {
            "transactionHash": "0x222",
            "blockNumber": 12351,
            "status": 1,
            "logs": [],
            "gasUsed": 21000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0

    def test_backward_compatibility(self):
        parser = AerodromeReceiptParser()

        swap_topic = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
        assert parser.is_aerodrome_event(swap_topic) is True
        assert parser.get_event_type(swap_topic) == AerodromeEventType.SWAP
