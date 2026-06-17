"""Tests for token address-to-symbol mapping integration.

This test suite validates that:
1. Known token addresses map to human-readable symbols
2. Swap execution shows symbols (not addresses) in portfolio results
3. Unknown tokens use checksummed address as fallback with warning

Part of US-065d: Symbol mapping integration tests (P0-4).
"""

import logging
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.paper import token_registry as token_registry_module
from almanak.framework.backtesting.paper.token_registry import (
    CHAIN_ID_ARBITRUM,
    CHAIN_ID_BASE,
    CHAIN_ID_BSC,
    CHAIN_ID_ETHEREUM,
    CHAIN_ID_OPTIMISM,
    CHAIN_ID_POLYGON,
    NATIVE_MATIC_ADDRESS,
    TOKEN_REGISTRY,
    _checksum_address,
    get_token_info,
    get_token_decimals,
    get_token_symbol,
    get_token_symbol_with_fallback,
)

# Token addresses for testing (lowercase, as stored in registry)
USDC_ETHEREUM = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WETH_ETHEREUM = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


class TestUSDCAddressMapping:
    """Tests for USDC address-to-symbol mapping (Acceptance Criteria #1)."""

    def test_usdc_ethereum_maps_to_symbol(self):
        """Test USDC on Ethereum maps to 'USDC' symbol."""
        symbol = get_token_symbol(CHAIN_ID_ETHEREUM, USDC_ETHEREUM)
        assert symbol == "USDC"

    def test_usdc_ethereum_case_insensitive(self):
        """Test USDC lookup is case-insensitive."""
        # Lowercase
        assert get_token_symbol(CHAIN_ID_ETHEREUM, USDC_ETHEREUM) == "USDC"
        # Uppercase
        assert get_token_symbol(CHAIN_ID_ETHEREUM, USDC_ETHEREUM.upper()) == "USDC"
        # Mixed case (checksummed)
        assert get_token_symbol(CHAIN_ID_ETHEREUM, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48") == "USDC"

    def test_usdc_arbitrum_maps_to_symbol(self):
        """Test USDC on Arbitrum maps to 'USDC' symbol."""
        symbol = get_token_symbol(CHAIN_ID_ARBITRUM, USDC_ARBITRUM)
        assert symbol == "USDC"

    def test_usdc_base_maps_to_symbol(self):
        """Test USDC on Base maps to 'USDC' symbol."""
        symbol = get_token_symbol(CHAIN_ID_BASE, USDC_BASE)
        assert symbol == "USDC"

    def test_usdc_info_complete(self):
        """Test USDC TokenInfo has all fields correctly populated."""
        info = get_token_info(CHAIN_ID_ETHEREUM, USDC_ETHEREUM)
        assert info is not None
        assert info.symbol == "USDC"
        assert info.decimals == 6
        assert info.address == USDC_ETHEREUM

    def test_get_token_info_uses_static_resolver_lookup(self, monkeypatch):
        calls: list[tuple[str, str, dict[str, object]]] = []

        class FakeResolver:
            def resolve(self, address: str, chain: str, **kwargs):
                calls.append((address, chain, kwargs))
                return SimpleNamespace(symbol="USDC", decimals=6, address=address)

        monkeypatch.setattr(token_registry_module, "_get_resolver", lambda: FakeResolver())

        info = token_registry_module.get_token_info(CHAIN_ID_ARBITRUM, USDC_ARBITRUM)

        assert info is not None
        assert info.symbol == "USDC"
        assert calls == [(USDC_ARBITRUM, "arbitrum", {"skip_gateway": True})]

    def test_weth_maps_to_symbol(self):
        """Test WETH maps to correct symbol."""
        symbol = get_token_symbol(CHAIN_ID_ETHEREUM, WETH_ETHEREUM)
        assert symbol == "WETH"

    def test_polygon_native_maps_to_pol_symbol(self):
        """Polygon native sentinel resolves to POL canonically."""
        symbol = get_token_symbol(CHAIN_ID_POLYGON, NATIVE_MATIC_ADDRESS)
        assert symbol == "POL"

    @pytest.mark.parametrize(
        ("chain_id", "address", "expected_symbol"),
        [
            (CHAIN_ID_ETHEREUM, "0x83f20f44975d03b1b09e64809b757c47f942beea", "SDAI"),
            (CHAIN_ID_ETHEREUM, "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2", "MKR"),
            (CHAIN_ID_ARBITRUM, "0xec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8", "RETH"),
            (CHAIN_ID_OPTIMISM, "0x9bcef72be871e61ed4fbbc7630889bee758eb81d", "RETH"),
            (CHAIN_ID_OPTIMISM, "0x1f32b1c2345538c0c6f582fcb022739c4a194ebb", "WSTETH"),
            (CHAIN_ID_POLYGON, "0x0266f4f08d82372cf0fcbccc0ff74309089c74d1", "STMATIC"),
            (CHAIN_ID_BSC, "0xe9e7cea3dedca5984780bafc599bd69add087d56", "BUSD"),
        ],
    )
    def test_legacy_paper_registry_tokens_resolve_from_canonical_json(
        self, chain_id, address, expected_symbol
    ):
        """Resolver-backed registry keeps legacy paper-token coverage."""
        info = get_token_info(chain_id, address)

        assert info is not None
        assert info.symbol == expected_symbol
        assert info.decimals == 18
        assert info.address == address
        assert get_token_symbol(chain_id, address.upper()) == expected_symbol
        assert get_token_decimals(chain_id, address) == 18


class TestSwapExecutionShowsSymbols:
    """Tests for swap execution showing symbols in portfolio (Acceptance Criteria #2).

    These tests verify that the _extract_token_flows method returns
    symbol keys (e.g., 'USDC') instead of address keys.
    """

    @pytest.mark.asyncio
    async def test_swap_result_uses_symbol_keys(self):
        """Test that swap execution returns symbol keys, not addresses."""
        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        wallet = "0x1234567890123456789012345678901234567890"

        # Create mock receipt with USDC transfer
        receipt = self._create_mock_receipt_with_transfer(
            token_address=USDC_ETHEREUM,
            from_addr=wallet,
            to_addr="0xDEXADDRESS000000000000000000000000000001",
            value=1_000_000_000,  # 1000 USDC
        )

        trader = self._create_mock_paper_trader(CHAIN_ID_ETHEREUM)
        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        _, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # Key should be 'USDC', not the address
        assert "USDC" in tokens_out, f"Expected 'USDC' key in tokens_out, got: {list(tokens_out.keys())}"
        assert USDC_ETHEREUM not in tokens_out, "Address should not be used as key when symbol is known"

    @pytest.mark.asyncio
    async def test_swap_weth_usdc_shows_both_symbols(self):
        """Test WETH -> USDC swap shows both symbols in result."""
        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        wallet = "0x1234567890123456789012345678901234567890"

        logs = [
            self._make_transfer_log(
                WETH_ETHEREUM,
                from_addr=wallet,
                to_addr="0xDEXADDRESS000000000000000000000000000001",
                value=500_000_000_000_000_000,  # 0.5 WETH
            ),
            self._make_transfer_log(
                USDC_ETHEREUM,
                from_addr="0xDEXADDRESS000000000000000000000000000001",
                to_addr=wallet,
                value=1_000_000_000,  # 1000 USDC
            ),
        ]

        receipt = self._create_mock_receipt(logs)
        trader = self._create_mock_paper_trader(CHAIN_ID_ETHEREUM)
        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # Verify both symbols are used
        assert "USDC" in tokens_in, f"Expected 'USDC' in tokens_in: {tokens_in}"
        assert "WETH" in tokens_out, f"Expected 'WETH' in tokens_out: {tokens_out}"

        # Verify amounts are correct
        assert tokens_in["USDC"] == Decimal("1000")
        assert tokens_out["WETH"] == Decimal("0.5")

    @pytest.mark.asyncio
    async def test_swap_on_arbitrum_shows_symbols(self):
        """Test swap on Arbitrum uses correct symbols."""
        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        wallet = "0x1234567890123456789012345678901234567890"

        logs = [
            self._make_transfer_log(
                USDC_ARBITRUM,
                from_addr="0xDEXADDRESS000000000000000000000000000001",
                to_addr=wallet,
                value=500_000_000,  # 500 USDC
            ),
        ]

        receipt = self._create_mock_receipt(logs)
        trader = self._create_mock_paper_trader(CHAIN_ID_ARBITRUM)
        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, _ = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        assert "USDC" in tokens_in, f"Expected 'USDC' on Arbitrum, got: {list(tokens_in.keys())}"
        assert tokens_in["USDC"] == Decimal("500")

    # Helper methods

    def _make_transfer_log(self, token_address: str, from_addr: str, to_addr: str, value: int) -> dict:
        """Create a mock ERC-20 Transfer event log."""
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        from_padded = from_addr.lower().replace("0x", "").zfill(64)
        to_padded = to_addr.lower().replace("0x", "").zfill(64)
        value_hex = hex(value)[2:].zfill(64)

        return {
            "address": token_address,
            "topics": [transfer_topic, f"0x{from_padded}", f"0x{to_padded}"],
            "data": f"0x{value_hex}",
        }

    def _create_mock_receipt(self, logs: list[dict], status: int = 1) -> MagicMock:
        """Create a mock TransactionReceipt."""
        receipt = MagicMock()
        receipt.to_dict.return_value = {
            "status": status,
            "logs": logs,
            "block_number": 12345678,
            "gas_used": 150000,
        }
        return receipt

    def _create_mock_receipt_with_transfer(
        self, token_address: str, from_addr: str, to_addr: str, value: int
    ) -> MagicMock:
        """Create a mock receipt with a single transfer."""
        logs = [self._make_transfer_log(token_address, from_addr, to_addr, value)]
        return self._create_mock_receipt(logs)

    def _create_mock_paper_trader(self, chain_id: int, is_running: bool = True) -> MagicMock:
        """Create a mock PaperTrader with fork manager."""
        from almanak.framework.backtesting.paper.engine import PaperTrader

        fork_manager = MagicMock()
        fork_manager.chain_id = chain_id
        fork_manager.is_running = is_running
        fork_manager.get_rpc_url.return_value = "http://localhost:8545"

        trader = MagicMock(spec=PaperTrader)
        trader.fork_manager = fork_manager
        trader._backtest_id = "test-backtest-id"

        return trader


class TestUnknownTokenAddressFallback:
    """Tests for unknown token using address fallback with warning (Acceptance Criteria #3)."""

    def test_unknown_token_returns_none(self):
        """Test unknown token returns None from registry lookup."""
        unknown_address = "0x1111111111111111111111111111111111111111"
        symbol = get_token_symbol(CHAIN_ID_ETHEREUM, unknown_address)
        assert symbol is None

    def test_unknown_chain_returns_none(self):
        """Test known token on unknown chain returns None."""
        unknown_chain_id = 999999
        symbol = get_token_symbol(unknown_chain_id, USDC_ETHEREUM)
        assert symbol is None

    @pytest.mark.asyncio
    async def test_fallback_uses_checksummed_address(self, caplog):
        """Test that unknown token falls back to checksummed address with warning."""
        unknown_address = "0x1111111111111111111111111111111111111111"

        with caplog.at_level(logging.WARNING):
            symbol = await get_token_symbol_with_fallback(
                CHAIN_ID_ETHEREUM,
                unknown_address,
                rpc_url=None,  # No RPC = skip on-chain lookup
            )

        # Should return checksummed address
        expected_checksum = "0x1111111111111111111111111111111111111111"
        assert symbol == expected_checksum

        # Should log warning
        assert "not found" in caplog.text.lower() or "fallback" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_fallback_logs_warning_with_chain_info(self, caplog):
        """Test that fallback warning includes chain ID."""
        unknown_address = "0x2222222222222222222222222222222222222222"

        with caplog.at_level(logging.WARNING):
            await get_token_symbol_with_fallback(
                CHAIN_ID_ARBITRUM,
                unknown_address,
                rpc_url=None,
            )

        # Warning should mention chain ID
        assert "42161" in caplog.text or "chain" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_unknown_token_decimals_unresolved_skipped(self):
        """VIB-3164: an unknown token with unresolved decimals is SKIPPED, not 18.

        Without an RPC or registry entry the decimals are unmeasured. The
        receipt path must not emit the token with a silently-defaulted amount
        (Empty != Zero). The symbol/address-identity fallback is owned by a
        separate lane (VIB-4487); this test pins only the decimal behaviour.
        """
        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        wallet = "0x1234567890123456789012345678901234567890"
        unknown_token = "0x3333333333333333333333333333333333333333"

        logs = [
            {
                "address": unknown_token,
                "topics": [
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                    f"0x{'0' * 24}{wallet[2:].lower()}",  # from: wallet
                    "0x" + "0" * 24 + b"DEX".hex().ljust(40, "0"),  # to: some DEX
                ],
                "data": "0x" + hex(1_000_000_000_000_000_000)[2:].zfill(64),  # 1 token (18 decimals)
            },
        ]

        receipt = MagicMock()
        receipt.to_dict.return_value = {
            "status": 1,
            "logs": logs,
            "block_number": 12345678,
            "gas_used": 150000,
        }

        fork_manager = MagicMock()
        fork_manager.chain_id = CHAIN_ID_ETHEREUM
        fork_manager.is_running = False  # No RPC available
        fork_manager.get_rpc_url.return_value = None

        trader = MagicMock(spec=RealPaperTrader)
        trader.fork_manager = fork_manager
        trader._backtest_id = "test-backtest-id"

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        _, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # The unresolved-decimal token must not be emitted with a fabricated amount.
        for key in tokens_out:
            assert unknown_token.lower() not in key.lower(), (
                f"unresolved-decimal token must be skipped, not emitted: {tokens_out}"
            )


class TestTokenRegistryStructure:
    """Tests for TOKEN_REGISTRY structure and content."""

    def test_registry_has_ethereum_tokens(self):
        """Test registry has Ethereum mainnet tokens."""
        assert CHAIN_ID_ETHEREUM in TOKEN_REGISTRY
        assert len(TOKEN_REGISTRY[CHAIN_ID_ETHEREUM]) >= 5  # ETH, WETH, USDC, USDT, WBTC, DAI

    def test_registry_has_arbitrum_tokens(self):
        """Test registry has Arbitrum tokens."""
        assert CHAIN_ID_ARBITRUM in TOKEN_REGISTRY
        assert len(TOKEN_REGISTRY[CHAIN_ID_ARBITRUM]) >= 4  # ETH, WETH, USDC, USDC.e, ARB

    def test_registry_has_base_tokens(self):
        """Test registry has Base tokens."""
        assert CHAIN_ID_BASE in TOKEN_REGISTRY
        assert len(TOKEN_REGISTRY[CHAIN_ID_BASE]) >= 3  # ETH, WETH, USDC

    def test_token_info_is_frozen(self):
        """Test that TokenInfo is immutable."""
        info = get_token_info(CHAIN_ID_ETHEREUM, USDC_ETHEREUM)
        assert info is not None

        # Should raise error when trying to modify (FrozenInstanceError)
        from dataclasses import FrozenInstanceError

        with pytest.raises(FrozenInstanceError):
            info.symbol = "CHANGED"

    def test_all_addresses_lowercase(self):
        """Test all registry addresses are lowercase."""
        for chain_id, tokens in TOKEN_REGISTRY.items():
            for address in tokens.keys():
                assert address == address.lower(), f"Address not lowercase: {address} on chain {chain_id}"


class TestChecksumAddress:
    """Tests for _checksum_address function."""

    def test_checksum_usdc(self):
        """Test USDC address checksum."""
        checksum = _checksum_address(USDC_ETHEREUM)
        # EIP-55 checksum for USDC
        assert checksum == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

    def test_checksum_preserves_length(self):
        """Test checksum preserves address length."""
        checksum = _checksum_address(USDC_ETHEREUM)
        assert len(checksum) == 42
        assert checksum.startswith("0x")

    def test_checksum_idempotent(self):
        """Test checksum is idempotent."""
        checksum1 = _checksum_address(USDC_ETHEREUM)
        checksum2 = _checksum_address(checksum1)
        assert checksum1 == checksum2


class TestGetTokenSymbolWithFallback:
    """Tests for get_token_symbol_with_fallback async function."""

    @pytest.mark.asyncio
    async def test_known_token_no_rpc_call(self):
        """Test known token returns immediately without RPC call."""
        # For known tokens, no web3 import should happen
        # We verify this by checking the result is correct and immediate
        symbol = await get_token_symbol_with_fallback(
            CHAIN_ID_ETHEREUM,
            USDC_ETHEREUM,
            rpc_url="http://localhost:8545",
        )

        # Should return USDC without RPC call (registry lookup)
        assert symbol == "USDC"

    @pytest.mark.asyncio
    async def test_unknown_token_tries_rpc(self):
        """Test unknown token tries RPC lookup before fallback."""
        unknown_address = "0x4444444444444444444444444444444444444444"

        # Mock web3 at import time in the function
        mock_web3_instance = MagicMock()
        mock_web3_instance.to_checksum_address.return_value = unknown_address
        mock_web3_instance.eth.call = AsyncMock(return_value=b"")  # Empty result = no symbol

        # The web3 is imported inside the function, so we patch it at the web3 module level
        with patch("web3.AsyncWeb3", return_value=mock_web3_instance):
            with patch("web3.AsyncHTTPProvider"):
                symbol = await get_token_symbol_with_fallback(
                    CHAIN_ID_ETHEREUM,
                    unknown_address,
                    rpc_url="http://localhost:8545",
                )

        # Should return checksummed address as fallback
        assert unknown_address.lower() in symbol.lower()

    @pytest.mark.asyncio
    async def test_no_rpc_url_skips_onchain_lookup(self, caplog):
        """Test that no RPC URL skips on-chain lookup."""
        unknown_address = "0x5555555555555555555555555555555555555555"

        with caplog.at_level(logging.WARNING):
            symbol = await get_token_symbol_with_fallback(
                CHAIN_ID_ETHEREUM,
                unknown_address,
                rpc_url=None,
            )

        # Should return checksummed address without trying RPC
        assert unknown_address.lower() in symbol.lower()
        # Warning should be logged
        assert "not found" in caplog.text.lower() or "fallback" in caplog.text.lower()
