"""Tests for FluidReceiptParser."""

from almanak.framework.connectors.fluid.receipt_parser import (
    ERC721_TRANSFER_TOPIC,
    LOG_OPERATE_TOPIC,
    ZERO_ADDRESS,
    FluidReceiptParser,
)


def _pad_uint256(value: int) -> str:
    return "0x" + hex(value)[2:].zfill(64)


def _pad_address(addr: str) -> str:
    return "0x" + addr[2:].lower().zfill(64)


def _encode_int256(value: int) -> str:
    if value >= 0:
        return hex(value)[2:].zfill(64)
    return hex((1 << 256) + value)[2:].zfill(64)


def _log_operate(nft_id: int, token0_amt: int, token1_amt: int, timestamp: int = 1000) -> dict:
    data = "0x" + _encode_int256(token0_amt) + _encode_int256(token1_amt) + hex(timestamp)[2:].zfill(64)
    return {
        "address": "0x1234567890123456789012345678901234567890",
        "topics": [LOG_OPERATE_TOPIC, _pad_uint256(nft_id)],
        "data": data,
        "logIndex": 0,
    }


def _erc721_mint(to_addr: str, token_id: int) -> dict:
    return {
        "address": "0x1234567890123456789012345678901234567890",
        "topics": [ERC721_TRANSFER_TOPIC, _pad_address(ZERO_ADDRESS), _pad_address(to_addr), _pad_uint256(token_id)],
        "data": "0x",
        "logIndex": 1,
    }


def _make_receipt(logs: list, status: int = 1) -> dict:
    return {"transactionHash": "0x" + "ab" * 32, "blockNumber": 12345, "status": status, "logs": logs}


class TestFluidReceiptParser:
    def setup_method(self):
        self.parser = FluidReceiptParser()

    def test_parse_lp_open_receipt(self):
        receipt = _make_receipt([_log_operate(nft_id=42, token0_amt=1_000_000, token1_amt=2_000_000)])
        result = self.parser.parse_receipt(receipt)
        assert result.success
        assert result.nft_id == 42
        assert result.token0_amt == 1_000_000
        assert result.token1_amt == 2_000_000

    def test_parse_lp_close_receipt(self):
        receipt = _make_receipt([_log_operate(nft_id=42, token0_amt=-500_000, token1_amt=-1_000_000)])
        result = self.parser.parse_receipt(receipt)
        assert result.success
        assert result.token0_amt == -500_000
        assert result.token1_amt == -1_000_000

    def test_erc721_mint_fallback(self):
        receipt = _make_receipt([_erc721_mint(to_addr="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", token_id=99)])
        result = self.parser.parse_receipt(receipt)
        assert result.nft_id == 99

    def test_reverted_transaction(self):
        result = self.parser.parse_receipt(_make_receipt([], status=0))
        assert not result.success

    def test_empty_logs(self):
        result = self.parser.parse_receipt(_make_receipt([]))
        assert not result.success

    def test_multiple_events_aggregate(self):
        receipt = _make_receipt([
            _log_operate(nft_id=42, token0_amt=100, token1_amt=200),
            _log_operate(nft_id=42, token0_amt=300, token1_amt=400),
        ])
        result = self.parser.parse_receipt(receipt)
        assert result.token0_amt == 400
        assert result.token1_amt == 600


class TestExtractPositionId:
    def setup_method(self):
        self.parser = FluidReceiptParser()

    def test_from_log_operate(self):
        receipt = _make_receipt([_log_operate(nft_id=42, token0_amt=1_000_000, token1_amt=2_000_000)])
        assert self.parser.extract_position_id(receipt) == 42

    def test_from_erc721_mint(self):
        receipt = _make_receipt([_erc721_mint(to_addr="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", token_id=77)])
        assert self.parser.extract_position_id(receipt) == 77

    def test_returns_none_on_failure(self):
        assert self.parser.extract_position_id(_make_receipt([], status=0)) is None


class TestExtractLPCloseData:
    def setup_method(self):
        self.parser = FluidReceiptParser()

    def test_extract_close_data(self):
        receipt = _make_receipt([_log_operate(nft_id=42, token0_amt=-500_000, token1_amt=-1_000_000)])
        close_data = self.parser.extract_lp_close_data(receipt)
        assert close_data is not None
        assert close_data.amount0_collected == 500_000
        assert close_data.amount1_collected == 1_000_000

    def test_reverted_returns_none(self):
        assert self.parser.extract_lp_close_data(_make_receipt([], status=0)) is None


class TestExtractLiquidity:
    def setup_method(self):
        self.parser = FluidReceiptParser()

    def test_open_returns_sum(self):
        receipt = _make_receipt([_log_operate(nft_id=42, token0_amt=1_000_000, token1_amt=2_000_000)])
        assert self.parser.extract_liquidity(receipt) == 3_000_000

    def test_close_returns_zero(self):
        receipt = _make_receipt([_log_operate(nft_id=42, token0_amt=-500_000, token1_amt=-1_000_000)])
        assert self.parser.extract_liquidity(receipt) == 0
