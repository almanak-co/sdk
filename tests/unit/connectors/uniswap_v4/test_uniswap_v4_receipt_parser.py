"""Tests for Uniswap V4 Receipt Parser."""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors.uniswap_v4.receipt_parser import (
    EVENT_TOPICS,
    ParseResult,
    SwapEventData,
    UniswapV4EventType,
    UniswapV4ReceiptParser,
)


# =============================================================================
# Helper: build mock receipts
# =============================================================================


def _encode_int128(value: int) -> str:
    """Encode int128 as 32-byte hex (two's complement for negative)."""
    if value < 0:
        value = (1 << 256) + value
    return hex(value)[2:].zfill(64)


def _encode_uint(value: int) -> str:
    """Encode uint256 as 32-byte hex."""
    return hex(value)[2:].zfill(64)


def _build_swap_log(
    amount0: int,
    amount1: int,
    sqrt_price_x96: int = 79228162514264337593543950336,
    liquidity: int = 10**18,
    tick: int = 0,
    fee: int = 3000,
    pool_id: str = "0x" + "ab" * 32,
    sender: str = "0x" + "00" * 12 + "1234567890123456789012345678901234567890",
) -> dict:
    """Build a mock V4 Swap event log."""
    data = (
        "0x"
        + _encode_int128(amount0)
        + _encode_int128(amount1)
        + _encode_uint(sqrt_price_x96)
        + _encode_uint(liquidity)
        + _encode_int128(tick)  # int24 but padded to 32 bytes
        + _encode_uint(fee)
    )
    return {
        "address": "0x000000000004444c5dc75cb358380d2e3de08a90",
        "topics": [
            EVENT_TOPICS["Swap"],
            pool_id,
            sender,
        ],
        "data": data,
    }


def _build_transfer_log(
    token: str,
    from_addr: str,
    to_addr: str,
    amount: int,
) -> dict:
    """Build a mock ERC-20 Transfer event log."""
    return {
        "address": token,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + from_addr.replace("0x", "").zfill(64),
            "0x" + to_addr.replace("0x", "").zfill(64),
        ],
        "data": "0x" + _encode_uint(amount),
    }


# =============================================================================
# Event topics tests
# =============================================================================


class TestEventTopics:
    def test_swap_topic_exists(self):
        assert "Swap" in EVENT_TOPICS
        assert EVENT_TOPICS["Swap"].startswith("0x")

    def test_swap_topic_keccak_matches(self):
        """Verify V4 PoolManager Swap topic matches keccak256 of the event signature."""
        from web3 import Web3

        expected = Web3.keccak(
            text="Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)"
        ).hex()
        assert EVENT_TOPICS["Swap"] == "0x" + expected, (
            f"V4 Swap topic mismatch: expected 0x{expected}, got {EVENT_TOPICS['Swap']}"
        )

    def test_modify_liquidity_topic_keccak_matches(self):
        """Verify V4 PoolManager ModifyLiquidity topic matches keccak256."""
        from web3 import Web3

        expected = Web3.keccak(
            text="ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)"
        ).hex()
        assert EVENT_TOPICS["ModifyLiquidity"] == "0x" + expected, (
            f"V4 ModifyLiquidity topic mismatch: expected 0x{expected}, got {EVENT_TOPICS['ModifyLiquidity']}"
        )

    def test_transfer_topic(self):
        assert EVENT_TOPICS["Transfer"] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


class TestEventTypes:
    def test_swap_type(self):
        assert UniswapV4EventType.SWAP.value == "SWAP"

    def test_modify_liquidity_type(self):
        assert UniswapV4EventType.MODIFY_LIQUIDITY.value == "MODIFY_LIQUIDITY"


# =============================================================================
# Parser initialization tests
# =============================================================================


class TestParserInit:
    def test_init_default(self):
        parser = UniswapV4ReceiptParser()
        assert parser.chain == "ethereum"

    def test_init_with_chain(self):
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        assert parser.chain == "arbitrum"
        assert parser.pool_manager == UNISWAP_V4["arbitrum"]["pool_manager"].lower()


# =============================================================================
# Receipt parsing tests
# =============================================================================


class TestParseReceipt:
    def test_parse_swap_receipt(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        # V4 sign convention (swapper's perspective):
        # amount0=+1000e6 = swapper received token0, amount1=-5e17 = swapper paid token1
        swap_log = _build_swap_log(
            amount0=1000 * 10**6,
            amount1=-(5 * 10**17),
        )
        receipt = {"logs": [swap_log]}

        result = parser.parse_receipt(receipt)

        assert len(result.swap_events) == 1
        assert result.swap_result is not None
        assert result.swap_result.amount_in == 5 * 10**17  # token1 paid
        assert result.swap_result.amount_out == 1000 * 10**6  # token0 received

    def test_parse_reverse_direction(self):
        """Test swap where token0 is paid, token1 is received."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        # amount0=-1000e6 = swapper paid token0, amount1=+5e17 = swapper received token1
        swap_log = _build_swap_log(
            amount0=-(1000 * 10**6),
            amount1=5 * 10**17,
        )
        receipt = {"logs": [swap_log]}

        result = parser.parse_receipt(receipt)

        assert result.swap_result is not None
        assert result.swap_result.amount_in == 1000 * 10**6  # token0 paid
        assert result.swap_result.amount_out == 5 * 10**17  # token1 received

    def test_parse_with_transfer_events(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        swap_log = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        transfer_log = _build_transfer_log(
            token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            from_addr="0x1111111111111111111111111111111111111111",
            to_addr="0x000000000004444c5dc75cb358380d2e3de08a90",
            amount=1000 * 10**6,
        )

        receipt = {"logs": [swap_log, transfer_log]}
        result = parser.parse_receipt(receipt)

        assert len(result.swap_events) == 1
        assert len(result.transfer_events) == 1

    def test_parse_empty_receipt(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        result = parser.parse_receipt({"logs": []})

        assert len(result.swap_events) == 0
        assert result.swap_result is None

    def test_slippage_calculation(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        # amount0=+1000e6 (received), amount1=-5e17 (paid)
        # amount_out = 1000e6 (received token0)
        swap_log = _build_swap_log(
            amount0=1000 * 10**6,
            amount1=-(5 * 10**17),
        )
        receipt = {"logs": [swap_log]}

        # Quote was 1100e6 but got 1000e6
        result = parser.parse_receipt(receipt, quoted_amount_out=1100 * 10**6)

        assert result.swap_result is not None
        assert result.swap_result.slippage_bps is not None
        assert result.swap_result.slippage_bps > 0

    def test_effective_price_without_transfers(self):
        """Without Transfer events, token decimals can't be resolved so effective_price is None."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        # amount0=+2000e6 (received), amount1=-1e18 (paid)
        swap_log = _build_swap_log(
            amount0=2000 * 10**6,
            amount1=-(1 * 10**18),
        )
        receipt = {"logs": [swap_log]}

        result = parser.parse_receipt(receipt)

        assert result.swap_result is not None
        assert result.swap_result.effective_price is None
        # amount_in = token1 paid, amount_out = token0 received
        assert result.swap_result.amount_in == 1 * 10**18
        assert result.swap_result.amount_out == 2000 * 10**6


class TestExtractSwapAmounts:
    def test_extract_swap_amounts(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        # amount0=+1000e6 (received), amount1=-5e17 (paid)
        swap_log = _build_swap_log(
            amount0=1000 * 10**6,
            amount1=-(5 * 10**17),
        )
        receipt = {"logs": [swap_log]}

        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.amount_in == 5 * 10**17  # token1 paid
        assert amounts.amount_out == 1000 * 10**6  # token0 received
        # Without Transfer events, decimals can't be resolved so effective_price falls back to 0
        assert amounts.effective_price == Decimal(0)

    def test_extract_no_swap(self):
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        amounts = parser.extract_swap_amounts({"logs": []})
        assert amounts is None

    def test_extract_with_transfer_events_pool_manager_match(self):
        """Transfer events to/from PoolManager enable token identification."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        pool_mgr = "0x000000000004444c5dc75cb358380d2e3de08a90"

        # amount0=+1000e6 (received), amount1=-5e17 (paid)
        # -> amount_in=5e17, amount_out=1000e6
        swap_log = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        # WETH transfer TO PoolManager (input — swapper paid token1=WETH)
        transfer_in = _build_transfer_log(
            token="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",  # WETH
            from_addr="0x1111111111111111111111111111111111111111",
            to_addr=pool_mgr,
            amount=5 * 10**17,
        )
        # USDC transfer FROM PoolManager (output — swapper received token0=USDC)
        transfer_out = _build_transfer_log(
            token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",  # USDC
            from_addr=pool_mgr,
            to_addr="0x1111111111111111111111111111111111111111",
            amount=1000 * 10**6,
        )

        receipt = {"logs": [swap_log, transfer_in, transfer_out]}
        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.amount_in == 5 * 10**17
        assert amounts.amount_out == 1000 * 10**6
        assert amounts.token_in == "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        assert amounts.token_out == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"

    def test_extract_with_transfer_events_amount_fallback(self):
        """When Transfers go via UniversalRouter (not directly to PoolManager),
        fall back to matching by amount."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        router = "0x66a9893cc07d91d95644aedd05d03f95e1dba8af"

        # amount0=+1000e6 (received), amount1=-5e17 (paid)
        swap_log = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        # WETH transfer to UniversalRouter — amount matches amount_in (5e17)
        transfer_in = _build_transfer_log(
            token="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",  # WETH
            from_addr="0x1111111111111111111111111111111111111111",
            to_addr=router,
            amount=5 * 10**17,
        )
        # USDC transfer from UniversalRouter — amount matches amount_out (1000e6)
        transfer_out = _build_transfer_log(
            token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",  # USDC
            from_addr=router,
            to_addr="0x1111111111111111111111111111111111111111",
            amount=1000 * 10**6,
        )

        receipt = {"logs": [swap_log, transfer_in, transfer_out]}
        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.amount_in == 5 * 10**17
        assert amounts.amount_out == 1000 * 10**6
        # Amount-based fallback should identify tokens
        assert amounts.token_in == "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        assert amounts.token_out == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"

    def test_extract_reverse_direction(self):
        """Reverse direction: token0 is paid, token1 is received."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        # amount0=-1000e6 (paid), amount1=+5e17 (received)
        swap_log = _build_swap_log(
            amount0=-(1000 * 10**6),
            amount1=5 * 10**17,
        )
        receipt = {"logs": [swap_log]}

        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.amount_in == 1000 * 10**6  # token0 paid
        assert amounts.amount_out == 5 * 10**17  # token1 received

    def test_extract_amount_fallback_equal_amounts(self):
        """When amount_in == amount_out (e.g. stablecoin-to-stablecoin swap),
        both tokens must still be identified by using different transfer events."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        router = "0x66a9893cc07d91d95644aedd05d03f95e1dba8af"
        amount = 1000 * 10**6  # Same raw amount for both USDC and USDT (6 decimals)

        # amount0=+amount (received), amount1=-amount (paid)
        swap_log = _build_swap_log(amount0=amount, amount1=-amount)
        # USDT transfer (input — paid) — same amount
        transfer_in = _build_transfer_log(
            token="0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",  # USDT
            from_addr="0x1111111111111111111111111111111111111111",
            to_addr=router,
            amount=amount,
        )
        # USDC transfer (output — received) — same amount, different token
        transfer_out = _build_transfer_log(
            token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",  # USDC
            from_addr=router,
            to_addr="0x1111111111111111111111111111111111111111",
            amount=amount,
        )

        receipt = {"logs": [swap_log, transfer_in, transfer_out]}
        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.amount_in == amount
        assert amounts.amount_out == amount
        # Both tokens must be identified despite equal amounts
        assert amounts.token_in == "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"
        assert amounts.token_out == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"

    def test_extract_amount_fallback_duplicate_transfers(self):
        """With multiple transfers of the same amount (Permit2 relay chain),
        the fallback picks distinct tokens for in and out."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        router = "0x66a9893cc07d91d95644aedd05d03f95e1dba8af"
        permit2 = "0x000000000022d473030f116ddee9f6b43ac78ba3"

        # amount0=+1000e6 (received), amount1=-5e17 (paid)
        swap_log = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        # Permit2 relay: user -> Permit2 (WETH amount matching amount_in)
        relay = _build_transfer_log(
            token="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",  # WETH
            from_addr="0x1111111111111111111111111111111111111111",
            to_addr=permit2,
            amount=5 * 10**17,
        )
        # Permit2 -> Router (same WETH amount)
        relay2 = _build_transfer_log(
            token="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",  # WETH
            from_addr=permit2,
            to_addr=router,
            amount=5 * 10**17,
        )
        # USDC output from Router
        transfer_out = _build_transfer_log(
            token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",  # USDC
            from_addr=router,
            to_addr="0x1111111111111111111111111111111111111111",
            amount=1000 * 10**6,
        )

        receipt = {"logs": [swap_log, relay, relay2, transfer_out]}
        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.token_in == "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        assert amounts.token_out == "0xaf88d065e77c8cc2239327c5edb3a432268e5831"

    def test_extract_single_transfer_event(self):
        """When only one Transfer event exists, only one side gets identified."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        router = "0x66a9893cc07d91d95644aedd05d03f95e1dba8af"

        # amount0=+1000e6 (received), amount1=-5e17 (paid)
        swap_log = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        # Only one transfer — matches amount_in (5e17, the paid amount)
        single_transfer = _build_transfer_log(
            token="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",  # WETH
            from_addr="0x1111111111111111111111111111111111111111",
            to_addr=router,
            amount=5 * 10**17,
        )

        receipt = {"logs": [swap_log, single_transfer]}
        amounts = parser.extract_swap_amounts(receipt)

        assert amounts is not None
        assert amounts.token_in == "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        assert amounts.token_out is None  # Only one Transfer, cannot identify output


# =============================================================================
# extract_position_id tests
# =============================================================================


def _build_erc721_transfer_log(
    contract_address: str,
    from_addr: str,
    to_addr: str,
    token_id: int,
) -> dict:
    """Build a mock ERC-721 Transfer event log (4 topics, no data)."""
    return {
        "address": contract_address,
        "topics": [
            EVENT_TOPICS["Transfer"],
            "0x" + from_addr.replace("0x", "").zfill(64),
            "0x" + to_addr.replace("0x", "").zfill(64),
            "0x" + hex(token_id)[2:].zfill(64),
        ],
        "data": "0x",
    }


class TestExtractPositionId:
    """Tests for extract_position_id — ERC-721 NFT tokenId extraction from LP mint receipts."""

    def test_extract_from_position_manager_mint(self):
        """Standard case: ERC-721 Transfer from PositionManager with from=zero (mint)."""
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        parser = UniswapV4ReceiptParser(chain="arbitrum")
        pm_addr = UNISWAP_V4["arbitrum"]["position_manager"]

        mint_log = _build_erc721_transfer_log(
            contract_address=pm_addr,
            from_addr="0x0000000000000000000000000000000000000000",
            to_addr="0x1111111111111111111111111111111111111111",
            token_id=42,
        )

        receipt = {"logs": [mint_log]}
        position_id = parser.extract_position_id(receipt)

        assert position_id == 42

    def test_extract_ignores_non_mint_transfers(self):
        """Regular transfers (from != zero) should NOT be extracted."""
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        parser = UniswapV4ReceiptParser(chain="arbitrum")
        pm_addr = UNISWAP_V4["arbitrum"]["position_manager"]

        # Transfer between two non-zero addresses (not a mint)
        transfer_log = _build_erc721_transfer_log(
            contract_address=pm_addr,
            from_addr="0x1111111111111111111111111111111111111111",
            to_addr="0x2222222222222222222222222222222222222222",
            token_id=99,
        )

        receipt = {"logs": [transfer_log]}
        assert parser.extract_position_id(receipt) is None

    def test_extract_returns_none_for_empty_receipt(self):
        """No logs -> no position ID."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        assert parser.extract_position_id({"logs": []}) is None

    def test_extract_with_mixed_logs(self):
        """Position ID extracted from ERC-721 mint among ERC-20 transfers and swaps."""
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        parser = UniswapV4ReceiptParser(chain="arbitrum")
        pm_addr = UNISWAP_V4["arbitrum"]["position_manager"]

        # ERC-20 Transfer (3 topics — should be skipped)
        erc20_transfer = _build_transfer_log(
            token="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            from_addr="0x1111111111111111111111111111111111111111",
            to_addr="0x2222222222222222222222222222222222222222",
            amount=1000 * 10**6,
        )

        # Swap event (should be skipped)
        swap_log = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))

        # ERC-721 mint from PositionManager (should be extracted)
        mint_log = _build_erc721_transfer_log(
            contract_address=pm_addr,
            from_addr="0x0000000000000000000000000000000000000000",
            to_addr="0x1111111111111111111111111111111111111111",
            token_id=12345,
        )

        receipt = {"logs": [erc20_transfer, swap_log, mint_log]}
        position_id = parser.extract_position_id(receipt)

        assert position_id == 12345

    def test_extract_fallback_known_v4_pm_different_chain(self):
        """When ERC-721 mint comes from a known V4 PositionManager address
        (but not the one configured for this chain), the fallback should
        still extract the position ID."""
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        parser = UniswapV4ReceiptParser(chain="arbitrum")
        # Use ethereum's PM address (known V4 PM, but not arbitrum's)
        eth_pm = UNISWAP_V4["ethereum"]["position_manager"]

        mint_log = _build_erc721_transfer_log(
            contract_address=eth_pm,
            from_addr="0x0000000000000000000000000000000000000000",
            to_addr="0x1111111111111111111111111111111111111111",
            token_id=777,
        )

        receipt = {"logs": [mint_log]}
        position_id = parser.extract_position_id(receipt)

        assert position_id == 777

    def test_extract_rejects_unknown_contract_mint(self):
        """ERC-721 mint from an unknown contract (not any known V4 PM)
        should be rejected — fail closed for money-critical field."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        mint_log = _build_erc721_transfer_log(
            contract_address="0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
            from_addr="0x0000000000000000000000000000000000000000",
            to_addr="0x1111111111111111111111111111111111111111",
            token_id=777,
        )

        receipt = {"logs": [mint_log]}
        assert parser.extract_position_id(receipt) is None

    def test_extract_prefers_position_manager_over_fallback(self):
        """When both the chain's PositionManager and another known V4 PM emit
        ERC-721 mints, prefer the chain's own PositionManager."""
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        parser = UniswapV4ReceiptParser(chain="arbitrum")
        pm_addr = UNISWAP_V4["arbitrum"]["position_manager"]
        # Use ethereum's PM as fallback candidate (known V4 PM, different chain)
        eth_pm = UNISWAP_V4["ethereum"]["position_manager"]

        fallback_mint = _build_erc721_transfer_log(
            contract_address=eth_pm,
            from_addr="0x0000000000000000000000000000000000000000",
            to_addr="0x1111111111111111111111111111111111111111",
            token_id=999,
        )

        # PositionManager mint (preferred)
        pm_mint = _build_erc721_transfer_log(
            contract_address=pm_addr,
            from_addr="0x0000000000000000000000000000000000000000",
            to_addr="0x1111111111111111111111111111111111111111",
            token_id=42,
        )

        receipt = {"logs": [fallback_mint, pm_mint]}
        position_id = parser.extract_position_id(receipt)

        assert position_id == 42

    def test_extract_large_token_id(self):
        """Large tokenId values should be handled correctly."""
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        parser = UniswapV4ReceiptParser(chain="arbitrum")
        pm_addr = UNISWAP_V4["arbitrum"]["position_manager"]

        large_id = 2**128 - 1  # Very large tokenId
        mint_log = _build_erc721_transfer_log(
            contract_address=pm_addr,
            from_addr="0x0000000000000000000000000000000000000000",
            to_addr="0x1111111111111111111111111111111111111111",
            token_id=large_id,
        )

        receipt = {"logs": [mint_log]}
        position_id = parser.extract_position_id(receipt)

        assert position_id == large_id

    def test_extract_all_supported_chains(self):
        """Position ID extraction works for all chains with V4 deployments."""
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        for chain_name, addrs in UNISWAP_V4.items():
            parser = UniswapV4ReceiptParser(chain=chain_name)
            pm_addr = addrs["position_manager"]

            mint_log = _build_erc721_transfer_log(
                contract_address=pm_addr,
                from_addr="0x0000000000000000000000000000000000000000",
                to_addr="0x1111111111111111111111111111111111111111",
                token_id=100,
            )

            receipt = {"logs": [mint_log]}
            position_id = parser.extract_position_id(receipt)

            assert position_id == 100, f"Failed for chain={chain_name}"

    def test_extract_case_insensitive_address(self):
        """Address comparison should be case-insensitive (checksum vs lowercase)."""
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        parser = UniswapV4ReceiptParser(chain="arbitrum")
        pm_addr = UNISWAP_V4["arbitrum"]["position_manager"]

        # Use all-uppercase address (simulating non-checksummed format)
        mint_log = _build_erc721_transfer_log(
            contract_address=pm_addr.upper(),
            from_addr="0x0000000000000000000000000000000000000000",
            to_addr="0x1111111111111111111111111111111111111111",
            token_id=55,
        )

        receipt = {"logs": [mint_log]}
        position_id = parser.extract_position_id(receipt)

        assert position_id == 55

    def test_extract_fails_closed_on_multiple_known_pm_mints(self):
        """When multiple ERC-721 mints from known V4 PMs exist (no exact chain match),
        return None to fail closed rather than guessing."""
        from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

        parser = UniswapV4ReceiptParser(chain="arbitrum")
        eth_pm = UNISWAP_V4["ethereum"]["position_manager"]
        base_pm = UNISWAP_V4["base"]["position_manager"]

        mint_a = _build_erc721_transfer_log(
            contract_address=eth_pm,
            from_addr="0x0000000000000000000000000000000000000000",
            to_addr="0x1111111111111111111111111111111111111111",
            token_id=111,
        )
        mint_b = _build_erc721_transfer_log(
            contract_address=base_pm,
            from_addr="0x0000000000000000000000000000000000000000",
            to_addr="0x1111111111111111111111111111111111111111",
            token_id=222,
        )

        receipt = {"logs": [mint_a, mint_b]}
        assert parser.extract_position_id(receipt) is None

    def test_extract_ignores_unknown_contract_mints_entirely(self):
        """ERC-721 mints from unknown contracts should never be fallback candidates."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")

        unknown_mint = _build_erc721_transfer_log(
            contract_address="0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            from_addr="0x0000000000000000000000000000000000000000",
            to_addr="0x1111111111111111111111111111111111111111",
            token_id=111,
        )

        receipt = {"logs": [unknown_mint]}
        assert parser.extract_position_id(receipt) is None


# =============================================================================
# Characterization tests for _build_swap_result
#
# These lock down the CURRENT behavior of the internal `_build_swap_result`
# method before the CC-51 refactor. The goal is an exhaustive safety net:
# every branch that influences `ParsedSwapResult` field semantics, sign
# conventions, token identification fallbacks, and decimal conversion is
# exercised here so that a subsequent phase-extraction refactor cannot
# silently alter receipt parsing.
# =============================================================================


class _StubResolvedToken:
    """Minimal stand-in for ResolvedToken used by token_resolver.resolve()."""

    def __init__(self, decimals: int) -> None:
        self.decimals = decimals


class _StubTokenResolver:
    """In-memory resolver that maps lowercased address -> decimals.

    Raises for unknown addresses (mirrors the real resolver behavior which
    raises when a token cannot be resolved on a chain).
    """

    def __init__(self, decimals_by_addr: dict[str, int]) -> None:
        self._decimals = {addr.lower(): d for addr, d in decimals_by_addr.items()}

    def resolve(self, token: str, chain: str, **kwargs):  # noqa: ARG002
        key = token.lower()
        if key not in self._decimals:
            raise LookupError(f"unknown token {token}")
        return _StubResolvedToken(self._decimals[key])


# Deterministic addresses used across characterization tests.
# _POOL_MGR_ARB is the REAL Arbitrum V4 PoolManager from
# almanak/core/contracts.py::UNISWAP_V4["arbitrum"]. Keeping this consistent
# is critical: tests that verify direction-fallback / pool-manager-match
# behavior must use the same address the parser resolves at construction.
_POOL_MGR_ARB = "0x360e68faccca8ca495c1b759fd9eee466db9fb32"
_USDC_ARB = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
_WETH_ARB = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
_USDT_ARB = "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"
_USER = "0x1111111111111111111111111111111111111111"
# Arbitrum UniversalRouter (UNISWAP_V4["arbitrum"]["universal_router"]).
_ROUTER = "0xa51afafe0263b40edaef0df8781ea9aa03e381a3"


class TestBuildSwapResultCharacterization:
    """Characterization suite for ``_build_swap_result``.

    These tests capture the behavior that must survive the refactor:
    sign convention, Swap event selection, token identification paths,
    decimal handling, and effective_price computation.
    """

    # -- Happy path: exact-input ---------------------------------------------

    def test_exact_input_direction_token1_in_token0_out(self):
        """amount0 positive -> swapper received token0, paid token1.

        Locks the canonical exact-input convention: amount_in = |amount1|,
        amount_out = amount0.
        """
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        swap_log = _build_swap_log(
            amount0=1000 * 10**6,
            amount1=-(5 * 10**17),
            tick=123,
            sqrt_price_x96=79228162514264337593543950336,
        )
        result = parser.parse_receipt({"logs": [swap_log]})

        assert result.swap_result is not None
        sr = result.swap_result
        assert sr.amount_in == 5 * 10**17
        assert sr.amount_out == 1000 * 10**6
        # tick_after / sqrt_price_x96_after must be carried from the Swap event
        assert sr.tick_after == 123
        assert sr.sqrt_price_x96_after == 79228162514264337593543950336

    # -- Exact-output direction (from pool's POV) ----------------------------

    def test_exact_output_direction_token0_in_token1_out(self):
        """amount0 negative -> swapper paid token0, received token1.

        V4 emits the final settlement amounts, so both exact-in and
        exact-out produce the same sign interpretation. This locks the
        reverse direction branch.
        """
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        swap_log = _build_swap_log(
            amount0=-(1000 * 10**6),
            amount1=5 * 10**17,
        )
        result = parser.parse_receipt({"logs": [swap_log]})

        assert result.swap_result is not None
        assert result.swap_result.amount_in == 1000 * 10**6
        assert result.swap_result.amount_out == 5 * 10**17

    # -- Multi-hop / multiple Swap events ------------------------------------

    def test_multi_hop_uses_first_swap_event(self):
        """With multiple Swap events, the FIRST one determines the result.

        This is intentional for multi-hop: the first hop's input is the
        user's input; the last hop's output is the user's output, but the
        current implementation only reads the first event. Locking that
        behavior so a future refactor cannot silently change it.
        """
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        first_hop = _build_swap_log(
            amount0=-(1000 * 10**6),  # paid 1000 USDC
            amount1=5 * 10**17,  # received intermediate
            tick=100,
        )
        second_hop = _build_swap_log(
            amount0=2 * 10**18,
            amount1=-(5 * 10**17),
            tick=200,
        )
        result = parser.parse_receipt({"logs": [first_hop, second_hop]})

        assert len(result.swap_events) == 2
        assert result.swap_result is not None
        # Values from FIRST event
        assert result.swap_result.amount_in == 1000 * 10**6
        assert result.swap_result.amount_out == 5 * 10**17
        assert result.swap_result.tick_after == 100

    # -- Reverted / missing Swap --------------------------------------------

    def test_reverted_tx_no_logs_no_swap_result(self):
        """A reverted tx emits no PoolManager logs -> swap_result stays None.

        The parser does NOT inspect receipt.status; it simply finds no
        Swap events and returns a ParseResult with swap_result=None.
        """
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        receipt = {"status": 0, "logs": []}
        result = parser.parse_receipt(receipt)

        assert result.swap_result is None
        assert result.swap_events == []

    def test_missing_swap_event_unrelated_logs(self):
        """Receipt with unrelated logs (only Transfers) produces no swap_result."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        transfer = _build_transfer_log(
            token=_USDC_ARB,
            from_addr=_USER,
            to_addr=_ROUTER,
            amount=1000 * 10**6,
        )
        result = parser.parse_receipt({"logs": [transfer]})

        assert result.swap_result is None
        assert len(result.transfer_events) == 1

    # -- Token identification paths -----------------------------------------

    def test_token_identification_pool_manager_direct(self):
        """Primary path: Transfer events that go directly to/from PoolManager."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        swap = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        t_in = _build_transfer_log(
            token=_WETH_ARB, from_addr=_USER, to_addr=_POOL_MGR_ARB, amount=5 * 10**17
        )
        t_out = _build_transfer_log(
            token=_USDC_ARB, from_addr=_POOL_MGR_ARB, to_addr=_USER, amount=1000 * 10**6
        )
        result = parser.parse_receipt({"logs": [swap, t_in, t_out]})

        assert result.swap_result is not None
        assert result.swap_result.token_in == _WETH_ARB
        assert result.swap_result.token_out == _USDC_ARB

    def test_token_identification_amount_fallback(self):
        """Secondary path: Transfers via a router identified by amount match."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        swap = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        t_in = _build_transfer_log(
            token=_WETH_ARB, from_addr=_USER, to_addr=_ROUTER, amount=5 * 10**17
        )
        t_out = _build_transfer_log(
            token=_USDC_ARB, from_addr=_ROUTER, to_addr=_USER, amount=1000 * 10**6
        )
        result = parser.parse_receipt({"logs": [swap, t_in, t_out]})

        assert result.swap_result is not None
        assert result.swap_result.token_in == _WETH_ARB
        assert result.swap_result.token_out == _USDC_ARB

    def test_token_identification_infra_direction_fallback(self):
        """Tertiary path: amounts differ due to wrap/unwrap; fallback by
        transfer direction relative to pool manager."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        # Swap amounts differ from actual ERC-20 flows (WETH wrap/unwrap scenario)
        swap = _build_swap_log(amount0=900 * 10**6, amount1=-(4 * 10**17))
        # FROM PoolManager -> output side
        t_out = _build_transfer_log(
            token=_USDC_ARB, from_addr=_POOL_MGR_ARB, to_addr=_USER, amount=123456
        )
        # TO PoolManager -> input side
        t_in = _build_transfer_log(
            token=_WETH_ARB, from_addr=_USER, to_addr=_POOL_MGR_ARB, amount=654321
        )
        result = parser.parse_receipt({"logs": [swap, t_out, t_in]})

        assert result.swap_result is not None
        # Direct pool_manager match still identifies both
        assert result.swap_result.token_in == _WETH_ARB
        assert result.swap_result.token_out == _USDC_ARB

    def test_token_identification_last_resort_deterministic_tiebreaker(self):
        """Last-resort path: neither PoolManager match nor amount match nor
        infra-direction match hits; remaining unseen tokens get assigned by
        a DETERMINISTIC, log-order-independent tiebreaker.

        Previously this branch used first-unseen-token -> output which
        depended on receipt log ordering (issue #1767). The fix sorts
        remaining tokens by lowercase address and assigns lowest-address
        -> output. For this fixture: WETH (0x82af...) < USDC (0xaf88...),
        so WETH -> output, USDC -> input regardless of log order.

        A deterministic guess is still a guess — the parser emits a
        WARNING when this branch fires.
        """
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        swap = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        # Neither endpoint is in the infra set; amounts don't match.
        # Both transfers flow between two unknown EOAs.
        t1 = _build_transfer_log(
            token=_USDC_ARB,
            from_addr="0x2222222222222222222222222222222222222222",
            to_addr="0x3333333333333333333333333333333333333333",
            amount=42,
        )
        t2 = _build_transfer_log(
            token=_WETH_ARB,
            from_addr="0x2222222222222222222222222222222222222222",
            to_addr="0x3333333333333333333333333333333333333333",
            amount=99,
        )
        result = parser.parse_receipt({"logs": [swap, t1, t2]})

        assert result.swap_result is not None
        # Deterministic tiebreaker: lowest lowercase address -> output
        assert result.swap_result.token_out == _WETH_ARB
        assert result.swap_result.token_in == _USDC_ARB

        # And log order must not change the outcome.
        result_reversed = parser.parse_receipt({"logs": [swap, t2, t1]})
        assert result_reversed.swap_result is not None
        assert result_reversed.swap_result.token_out == _WETH_ARB
        assert result_reversed.swap_result.token_in == _USDC_ARB

    def test_router_routed_direction_fallback_resolves_correctly(self):
        """Regression for #1767.

        When ERC-20 Transfer amounts diverge from Swap event amounts
        (WRAP_ETH / UNWRAP_WETH via UniversalRouter), the direction
        fallback must still identify sides correctly via the broadened
        ``_infra_addresses`` set (pool_manager + position_manager +
        universal_router + Permit2 + wrapped-native).

        Pre-fix behavior (issue #1767): with INPUT-side transfer logged
        first, elimination silently FLIPPED token_in / token_out. Fix:
        UniversalRouter is now in the infra set, so the directional pass
        resolves WETH->input / USDC->output regardless of log order.
        """
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        # amount0=+1000 USDC (received), amount1=-0.5 WETH (paid) per V4 Swap.
        swap = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        # Wrapped-ETH path: user's WETH transfer amount differs from the V4
        # Swap amount (0.6 WETH wrapped, only 0.5 consumed); USDC payout also
        # differs (router returns dust). Neither amount matches the Swap
        # event's amount_in / amount_out; both transfers flow to/from the
        # UniversalRouter. With the fix, direction pass identifies sides.
        input_side = _build_transfer_log(
            token=_WETH_ARB,
            from_addr=_USER,
            to_addr=_ROUTER,
            amount=6 * 10**17,  # 0.6 WETH — ≠ amount_in (5e17)
        )
        output_side = _build_transfer_log(
            token=_USDC_ARB,
            from_addr=_ROUTER,
            to_addr=_USER,
            amount=999 * 10**6,  # ≠ amount_out (1000e6)
        )
        receipt = {"logs": [swap, input_side, output_side]}
        result = parser.parse_receipt(receipt)

        assert result.swap_result is not None
        sr = result.swap_result
        # Correct side assignment from directional pass (router in infra).
        assert sr.token_in == _WETH_ARB
        assert sr.token_out == _USDC_ARB

    def test_router_routed_direction_fallback_is_log_order_independent(self):
        """Regression for #1767 — log order must not affect side assignment.

        Same scenario as the previous test, but with output-side logged
        first. Pre-fix: log-order elimination produced the "correct"
        answer here but the flipped answer when input-side came first;
        the fix eliminates the log-order dependency entirely.
        """
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        swap = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        input_side = _build_transfer_log(
            token=_WETH_ARB, from_addr=_USER, to_addr=_ROUTER, amount=6 * 10**17
        )
        output_side = _build_transfer_log(
            token=_USDC_ARB, from_addr=_ROUTER, to_addr=_USER, amount=999 * 10**6
        )
        # Reversed log order vs previous test
        result = parser.parse_receipt({"logs": [swap, output_side, input_side]})
        assert result.swap_result is not None
        assert result.swap_result.token_in == _WETH_ARB
        assert result.swap_result.token_out == _USDC_ARB

    def test_direction_fallback_ignores_infra_to_infra_hops(self):
        """Regression: infra-to-infra Transfers in the direction fallback
        carry no directional information and must NOT be classified as
        user output/input.

        Flagged by Gemini / Codex on PR #1774: with the broadened infra
        set, a Permit2 -> UniversalRouter or WETH-contract -> Router
        internal routing leg could be misclassified as user output
        because ``from_lower in infra``. The fix requires EXACTLY ONE
        side to be non-infra.

        Test construction: a dummy token flows Permit2 -> Router (both
        infra, neither the pool_manager, so the primary pool-manager
        pass doesn't consume it). Without the guard, the direction
        pass would eat this hop as token_out and then the real USDC
        output leg would slot into the remaining slot via the
        last-resort tiebreaker, flipping sides.
        """
        _PERMIT2 = "0x000000000022d473030f116ddee9f6b43ac78ba3"
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        # Amounts deliberately differ from swap amounts so the amount
        # fallback also misses and we end up in the direction pass.
        swap = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        infra_hop = _build_transfer_log(
            token="0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead",
            from_addr=_PERMIT2,
            to_addr=_ROUTER,  # infra -> infra, and neither is pool_manager
            amount=7,
        )
        # Legitimate user legs via ROUTER (one side infra, one side user).
        input_side = _build_transfer_log(
            token=_WETH_ARB, from_addr=_USER, to_addr=_ROUTER, amount=6 * 10**17
        )
        output_side = _build_transfer_log(
            token=_USDC_ARB, from_addr=_ROUTER, to_addr=_USER, amount=999 * 10**6
        )
        # Infra hop BEFORE user legs to maximize misclassification risk.
        result = parser.parse_receipt({"logs": [swap, infra_hop, input_side, output_side]})
        assert result.swap_result is not None
        assert result.swap_result.token_in == _WETH_ARB
        assert result.swap_result.token_out == _USDC_ARB

    # -- Decimal / precision handling ---------------------------------------

    def test_decimal_conversion_with_resolver(self):
        """When token_resolver resolves BOTH tokens, amount_in_decimal,
        amount_out_decimal, and effective_price are all computed, and the
        *_decimal_resolved flags are True (issue #1778)."""
        resolver = _StubTokenResolver({_USDC_ARB: 6, _WETH_ARB: 18})
        parser = UniswapV4ReceiptParser(chain="arbitrum", token_resolver=resolver)
        swap = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        t_in = _build_transfer_log(
            token=_WETH_ARB, from_addr=_USER, to_addr=_POOL_MGR_ARB, amount=5 * 10**17
        )
        t_out = _build_transfer_log(
            token=_USDC_ARB, from_addr=_POOL_MGR_ARB, to_addr=_USER, amount=1000 * 10**6
        )
        result = parser.parse_receipt({"logs": [swap, t_in, t_out]})

        assert result.swap_result is not None
        sr = result.swap_result
        assert sr.amount_in_decimal == Decimal("0.5")  # 0.5 WETH
        assert sr.amount_out_decimal == Decimal("1000")  # 1000 USDC
        assert sr.effective_price == Decimal("2000")  # 1000 USDC / 0.5 WETH
        # Both sides resolved -> flags are True
        assert sr.amount_in_decimal_resolved is True
        assert sr.amount_out_decimal_resolved is True

        # extract_swap_amounts must copy the resolved flags forward so the
        # ledger sees the same truth (regression guard for #1778).
        sa = parser.extract_swap_amounts({"logs": [swap, t_in, t_out]})
        assert sa is not None
        assert sa.amount_in_decimal_resolved is True
        assert sa.amount_out_decimal_resolved is True

    def test_decimal_conversion_unresolved_falls_back_to_zero(self):
        """When decimals cannot be resolved, decimal fields fall back to
        Decimal(0) as a backward-compatible sentinel, but the
        *_decimal_resolved flags are False so downstream consumers can
        distinguish this from a measured zero (issue #1778)."""
        # Resolver raises on both addresses
        resolver = _StubTokenResolver({})
        parser = UniswapV4ReceiptParser(chain="arbitrum", token_resolver=resolver)
        swap = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        t_in = _build_transfer_log(
            token=_WETH_ARB, from_addr=_USER, to_addr=_POOL_MGR_ARB, amount=5 * 10**17
        )
        t_out = _build_transfer_log(
            token=_USDC_ARB, from_addr=_POOL_MGR_ARB, to_addr=_USER, amount=1000 * 10**6
        )
        result = parser.parse_receipt({"logs": [swap, t_in, t_out]})

        assert result.swap_result is not None
        sr = result.swap_result
        # Raw integers preserved
        assert sr.amount_in == 5 * 10**17
        assert sr.amount_out == 1000 * 10**6
        # Human-readable fields guard: fall back to Decimal(0)
        assert sr.amount_in_decimal == Decimal(0)
        assert sr.amount_out_decimal == Decimal(0)
        # effective_price must be None when decimals are missing
        assert sr.effective_price is None
        # NEW (#1778): flags must be False so downstream can tell this
        # apart from a legitimately measured zero.
        assert sr.amount_in_decimal_resolved is False
        assert sr.amount_out_decimal_resolved is False

        # extract_swap_amounts must propagate both False flags so the
        # ledger doesn't mistake sentinel zeros for measured zeros.
        sa = parser.extract_swap_amounts({"logs": [swap, t_in, t_out]})
        assert sa is not None
        assert sa.amount_in_decimal_resolved is False
        assert sa.amount_out_decimal_resolved is False

    def test_decimal_conversion_partial_resolution_skips_price(self):
        """If only one side's decimals resolve, effective_price must be None
        to avoid mixing raw integers with decimal amounts (off by orders
        of magnitude for cross-decimal pairs). The *_decimal_resolved
        flags must reflect each side independently (issue #1778)."""
        resolver = _StubTokenResolver({_USDC_ARB: 6})  # WETH missing
        parser = UniswapV4ReceiptParser(chain="arbitrum", token_resolver=resolver)
        swap = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        t_in = _build_transfer_log(
            token=_WETH_ARB, from_addr=_USER, to_addr=_POOL_MGR_ARB, amount=5 * 10**17
        )
        t_out = _build_transfer_log(
            token=_USDC_ARB, from_addr=_POOL_MGR_ARB, to_addr=_USER, amount=1000 * 10**6
        )
        result = parser.parse_receipt({"logs": [swap, t_in, t_out]})

        assert result.swap_result is not None
        sr = result.swap_result
        # USDC side resolved -> decimal populated
        assert sr.amount_out_decimal == Decimal("1000")
        assert sr.amount_out_decimal_resolved is True
        # WETH side NOT resolved -> fallback zero, flag False
        assert sr.amount_in_decimal == Decimal(0)
        assert sr.amount_in_decimal_resolved is False
        # effective_price requires BOTH resolved
        assert sr.effective_price is None

        # extract_swap_amounts must propagate the partial flags so the
        # ledger can distinguish unresolved-in from measured-in.
        sa = parser.extract_swap_amounts({"logs": [swap, t_in, t_out]})
        assert sa is not None
        assert sa.amount_out_decimal_resolved is True
        assert sa.amount_in_decimal_resolved is False

    # -- Slippage -----------------------------------------------------------

    def test_slippage_bps_positive(self):
        """Slippage = (quoted - actual) / quoted; positive when we got less."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        swap = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        result = parser.parse_receipt(
            {"logs": [swap]}, quoted_amount_out=1100 * 10**6
        )

        assert result.swap_result is not None
        # (1100 - 1000) / 1100 = 0.0909... * 10000 -> 909
        assert result.swap_result.slippage_bps == 909

    def test_slippage_bps_none_without_quote(self):
        """Without quoted_amount_out, slippage_bps stays None."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        swap = _build_swap_log(amount0=1000 * 10**6, amount1=-(5 * 10**17))
        result = parser.parse_receipt({"logs": [swap]})

        assert result.swap_result is not None
        assert result.swap_result.slippage_bps is None

    # -- Preserved dataclass contract ---------------------------------------

    def test_parsed_swap_result_fields_preserved(self):
        """Lock the ParsedSwapResult field surface that downstream relies on."""
        parser = UniswapV4ReceiptParser(chain="arbitrum")
        swap = _build_swap_log(
            amount0=1000 * 10**6,
            amount1=-(5 * 10**17),
            tick=17,
            sqrt_price_x96=42,
        )
        result = parser.parse_receipt({"logs": [swap]})

        assert result.swap_result is not None
        sr = result.swap_result
        # Every field documented on ParsedSwapResult must be set (even if None).
        for attr in (
            "amount_in",
            "amount_out",
            "amount_in_decimal",
            "amount_out_decimal",
            "token_in",
            "token_out",
            "effective_price",
            "price_impact_bps",
            "slippage_bps",
            "tick_after",
            "sqrt_price_x96_after",
        ):
            assert hasattr(sr, attr), f"ParsedSwapResult missing {attr}"
        assert sr.tick_after == 17
        assert sr.sqrt_price_x96_after == 42
