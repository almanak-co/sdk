"""Tests for paper trading wallet bootstrapping fixes.

Covers:
- ERC-20 address checksumming (not uppercasing) in backtest CLI
- Token symbol case preservation (wstETH, swETH, USDbC, wS)
- PaperTraderConfig.get_initial_balances() preserves original case
- RollingForkManager.fund_tokens() case-insensitive symbol resolution
- RollingForkManager._fetch_decimals_onchain() fallback
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.anvil.fork_manager import RollingForkManager
from almanak.framework.backtesting.paper.config import PaperTraderConfig

# ---- PaperTraderConfig.get_initial_balances() ----


class TestGetInitialBalances:
    """get_initial_balances() must preserve original token key casing."""

    def test_preserves_mixed_case_symbols(self):
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example.com",
            strategy_id="test",
            initial_tokens={
                "wstETH": Decimal("1.0"),
                "USDC": Decimal("100"),
                "USDbC": Decimal("50"),
            },
        )
        balances = config.get_initial_balances()
        assert "wstETH" in balances
        assert "USDC" in balances
        assert "USDbC" in balances
        # Must NOT have uppercased versions
        assert "WSTETH" not in balances
        assert "USDBC" not in balances

    def test_preserves_checksummed_address(self):
        config = PaperTraderConfig(
            chain="ethereum",
            rpc_url="https://eth.example.com",
            strategy_id="test",
            initial_tokens={
                "0xf951E335afb289353dc249e82926178EaC7DEd78": Decimal("0.01"),
            },
        )
        balances = config.get_initial_balances()
        assert "0xf951E335afb289353dc249e82926178EaC7DEd78" in balances
        # Must NOT have uppercased address (which would break resolver)
        assert "0XF951E335AFB289353DC249E82926178EAC7DED78" not in balances

    def test_always_includes_eth(self):
        config = PaperTraderConfig(
            chain="ethereum",
            rpc_url="https://eth.example.com",
            strategy_id="test",
            initial_eth=Decimal("5"),
            initial_tokens={"wstETH": Decimal("1.0")},
        )
        balances = config.get_initial_balances()
        assert balances["ETH"] == Decimal("5")
        assert balances["wstETH"] == Decimal("1.0")


# ---- RollingForkManager.fund_tokens() ----


class TestFundTokens:
    """fund_tokens() must handle raw addresses and case-insensitive symbols."""

    @pytest.fixture()
    def manager(self):
        mgr = RollingForkManager(rpc_url="http://rpc.test", chain="ethereum", anvil_port=9999)
        mgr._is_running = True
        # Mock _process so is_running property returns True
        mock_process = MagicMock()
        mock_process.poll.return_value = None  # process still running
        mgr._process = mock_process
        return mgr

    @pytest.mark.asyncio()
    async def test_raw_address_resolves_via_resolver(self, manager):
        """Raw ERC-20 address should be resolved by TokenResolver for decimals."""
        mock_resolved = MagicMock(address="0xf951e335afb289353dc249e82926178eac7ded78", decimals=18, symbol="swETH")
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = mock_resolved

        with (
            patch("almanak.framework.data.tokens.get_token_resolver", return_value=mock_resolver),
            patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, return_value=(True, None)),
        ):
            result = await manager.fund_tokens(
                "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
                {"0xf951E335afb289353dc249e82926178EaC7DEd78": Decimal("1.0")},
            )
            assert result is True
            # Resolver should be called with the original address
            mock_resolver.resolve.assert_called_once()

    @pytest.mark.asyncio()
    async def test_raw_address_falls_back_to_onchain_decimals(self, manager):
        """When resolver fails for raw address, on-chain decimals() should be used."""
        from almanak.framework.data.tokens.exceptions import TokenNotFoundError

        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = TokenNotFoundError(
            token="0xf951E335afb289353dc249e82926178EaC7DEd78", chain="ethereum"
        )

        # _rpc_call_raw returns: decimals call -> 18, then anvil_deal -> success
        async def mock_rpc(method, params):
            if method == "eth_call":
                return (True, hex(18))  # decimals() returns 18
            if method == "anvil_deal":
                return (True, None)
            return (False, None)

        with (
            patch("almanak.framework.data.tokens.get_token_resolver", return_value=mock_resolver),
            patch.object(manager, "_rpc_call_raw", side_effect=mock_rpc),
        ):
            result = await manager.fund_tokens(
                "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
                {"0xf951E335afb289353dc249e82926178EaC7DEd78": Decimal("1.0")},
            )
            assert result is True

    @pytest.mark.asyncio()
    async def test_case_insensitive_symbol_lookup(self, manager):
        """Symbols like 'wstETH' should resolve even if TOKEN_ADDRESSES has different casing."""
        mock_resolved = MagicMock(
            address="0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", decimals=18, symbol="wstETH"
        )
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = mock_resolved

        with (
            patch("almanak.framework.data.tokens.get_token_resolver", return_value=mock_resolver),
            patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, return_value=(True, None)),
        ):
            result = await manager.fund_tokens(
                "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
                {"wstETH": Decimal("1.0")},
            )
            assert result is True

    @pytest.mark.asyncio()
    async def test_uppercase_0x_treated_as_address(self, manager):
        """'0X...' (uppercase X) must still be treated as a raw address, not a symbol."""
        mock_resolved = MagicMock(address="0xf951e335afb289353dc249e82926178eac7ded78", decimals=18, symbol="swETH")
        mock_resolver = MagicMock()
        mock_resolver.resolve.return_value = mock_resolved

        with (
            patch("almanak.framework.data.tokens.get_token_resolver", return_value=mock_resolver),
            patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, return_value=(True, None)),
        ):
            # "0X..." with uppercase X — was the root cause of the bug
            result = await manager.fund_tokens(
                "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
                {"0XF951E335AFB289353DC249E82926178EAC7DED78": Decimal("1.0")},
            )
            assert result is True

    @pytest.mark.asyncio()
    async def test_not_running_returns_false(self, manager):
        manager._is_running = False
        result = await manager.fund_tokens("0x" + "a" * 40, {"USDC": Decimal("100")})
        assert result is False


# ---- RollingForkManager._fetch_decimals_onchain() ----


class TestFetchDecimalsOnchain:
    """_fetch_decimals_onchain() must parse the eth_call response correctly."""

    @pytest.fixture()
    def manager(self):
        return RollingForkManager(rpc_url="http://rpc.test", chain="ethereum", anvil_port=9999)

    @pytest.mark.asyncio()
    async def test_returns_decimals_from_hex(self, manager):
        with patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, return_value=(True, hex(6))):
            result = await manager._fetch_decimals_onchain("0x" + "a" * 40)
            assert result == 6

    @pytest.mark.asyncio()
    async def test_returns_18_for_standard_token(self, manager):
        with patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, return_value=(True, hex(18))):
            result = await manager._fetch_decimals_onchain("0x" + "a" * 40)
            assert result == 18

    @pytest.mark.asyncio()
    async def test_returns_none_on_failure(self, manager):
        with patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, return_value=(False, None)):
            result = await manager._fetch_decimals_onchain("0x" + "a" * 40)
            assert result is None

    @pytest.mark.asyncio()
    async def test_returns_none_on_exception(self, manager):
        with patch.object(manager, "_rpc_call_raw", new_callable=AsyncMock, side_effect=Exception("rpc error")):
            result = await manager._fetch_decimals_onchain("0x" + "a" * 40)
            assert result is None


# ---- CLI anvil_funding address handling ----


class TestAnvilFundingAddressParsing:
    """Verify address checksumming logic matches what backtest.py does."""

    def test_checksum_preserves_case_correctly(self):
        """EIP-55 checksum must produce correct mixed-case address."""
        from eth_utils import to_checksum_address

        # swETH address — lowercase input
        raw = "0xf951e335afb289353dc249e82926178eac7ded78"
        checksummed = to_checksum_address(raw)
        assert checksummed == "0xf951E335afb289353dc249e82926178EaC7DEd78"

        # Must NOT be all-uppercase
        assert checksummed != raw.upper()

        # Must start with lowercase 0x
        assert checksummed.startswith("0x")

    def test_checksum_is_idempotent(self):
        """Checksumming an already-checksummed address returns the same result."""
        from eth_utils import to_checksum_address

        addr = "0xf951E335afb289353dc249e82926178EaC7DEd78"
        assert to_checksum_address(addr) == addr

    def test_address_detection(self):
        """Verify the address detection logic used in backtest.py."""
        # Valid addresses
        assert "0xf951E335afb289353dc249e82926178EaC7DEd78".startswith(("0x", "0X"))
        assert len("0xf951E335afb289353dc249e82926178EaC7DEd78") == 42

        # Uppercased address (the broken path)
        uppercased = "0XF951E335AFB289353DC249E82926178EAC7DED78"
        assert uppercased.startswith(("0x", "0X"))
        assert len(uppercased) == 42

        # Symbols must NOT match
        assert not ("USDC".startswith(("0x", "0X")) and len("USDC") == 42)
        assert not ("wstETH".startswith(("0x", "0X")) and len("wstETH") == 42)



# ---- PaperTraderConfig.bootstrap field (VIB-2375) ----


class TestBootstrapConfig:
    """bootstrap field: per-chain token requirements merged into get_initial_balances()."""

    def test_bootstrap_merges_into_balances(self):
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example.com",
            strategy_id="test",
            bootstrap={
                "arbitrum": {"USDC": Decimal("100"), "WETH": Decimal("1")},
                "ethereum": {"USDT": Decimal("50")},
            },
        )
        balances = config.get_initial_balances()
        assert balances["USDC"] == Decimal("100")
        assert balances["WETH"] == Decimal("1")
        # Ethereum tokens NOT included (wrong chain)
        assert "USDT" not in balances

    def test_initial_tokens_override_bootstrap(self):
        """initial_tokens (CLI) take precedence over bootstrap (config)."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example.com",
            strategy_id="test",
            initial_tokens={"USDC": Decimal("500")},
            bootstrap={
                "arbitrum": {"USDC": Decimal("100"), "WETH": Decimal("1")},
            },
        )
        balances = config.get_initial_balances()
        # initial_tokens overrides bootstrap for USDC
        assert balances["USDC"] == Decimal("500")
        # bootstrap's WETH still included
        assert balances["WETH"] == Decimal("1")

    def test_empty_bootstrap_no_effect(self):
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example.com",
            strategy_id="test",
            initial_tokens={"USDC": Decimal("100")},
            bootstrap={},
        )
        balances = config.get_initial_balances()
        assert balances["USDC"] == Decimal("100")
        assert balances["ETH"] == Decimal("10")  # default

    def test_bootstrap_wrong_chain_ignored(self):
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example.com",
            strategy_id="test",
            bootstrap={
                "ethereum": {"USDT": Decimal("50")},
            },
        )
        balances = config.get_initial_balances()
        assert "USDT" not in balances

    def test_bootstrap_validation_rejects_negative(self):
        with pytest.raises(ValueError, match=r"bootstrap.*cannot be negative"):
            PaperTraderConfig(
                chain="arbitrum",
                rpc_url="https://arb.example.com",
                strategy_id="test",
                bootstrap={"arbitrum": {"USDC": Decimal("-100")}},
            )

    def test_bootstrap_serialization_roundtrip(self):
        """bootstrap survives to_dict() -> from_dict() roundtrip."""
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example.com",
            strategy_id="test",
            bootstrap={
                "arbitrum": {"USDC": Decimal("100"), "wstETH": Decimal("1.5")},
                "ethereum": {"USDT": Decimal("50")},
            },
        )
        data = config.to_dict()
        assert "bootstrap" in data
        assert data["bootstrap"]["arbitrum"]["USDC"] == "100"
        assert data["bootstrap"]["ethereum"]["USDT"] == "50"

        restored = PaperTraderConfig.from_dict({
            "chain": "arbitrum",
            "rpc_url": "https://arb.example.com",
            "strategy_id": "test",
            **{k: v for k, v in data.items() if k not in ("chain", "rpc_url", "strategy_id", "chain_id", "max_duration_seconds", "fork_rpc_url", "allow_hardcoded_fallback")},
        })
        assert restored.bootstrap["arbitrum"]["USDC"] == Decimal("100")
        assert restored.bootstrap["ethereum"]["USDT"] == Decimal("50")

    def test_bootstrap_preserves_mixed_case_symbols(self):
        config = PaperTraderConfig(
            chain="ethereum",
            rpc_url="https://eth.example.com",
            strategy_id="test",
            bootstrap={
                "ethereum": {"wstETH": Decimal("1"), "swETH": Decimal("0.5")},
            },
        )
        balances = config.get_initial_balances()
        assert "wstETH" in balances
        assert "swETH" in balances
