"""Tests for the synthetic intent factory."""

import pytest

from almanak.framework.intents.compiler import (
    LENDING_POOL_ADDRESSES,
    LP_POSITION_MANAGERS,
    PROTOCOL_ROUTERS,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import (
    BorrowIntent,
    CollectFeesIntent,
    FlashLoanIntent,
    LPCloseIntent,
    LPOpenIntent,
    PerpCloseIntent,
    PerpOpenIntent,
    RepayIntent,
    SupplyIntent,
    SwapIntent,
    VaultDepositIntent,
    VaultRedeemIntent,
    WithdrawIntent,
)
from almanak.framework.permissions.hints import PermissionHints, get_permission_hints
from almanak.framework.permissions.synthetic_intents import build_synthetic_intents


class TestSwapIntents:
    """Test synthetic SWAP intent creation."""

    def test_uniswap_v3_arbitrum(self):
        """uniswap_v3 on arbitrum should produce a SwapIntent."""
        intents = build_synthetic_intents("uniswap_v3", "SWAP", "arbitrum")
        assert len(intents) == 1
        assert isinstance(intents[0], SwapIntent)
        assert intents[0].protocol == "uniswap_v3"
        assert intents[0].chain == "arbitrum"

    def test_aerodrome_base(self):
        """aerodrome on base should produce a SwapIntent."""
        intents = build_synthetic_intents("aerodrome", "SWAP", "base")
        assert len(intents) == 1
        assert isinstance(intents[0], SwapIntent)

    def test_traderjoe_v2_avalanche(self):
        """traderjoe_v2 swap on avalanche returns empty -- blocked at compiler (VIB-1406).

        LBRouter2 interface is incompatible with DefaultSwapAdapter. Swaps must
        fail-closed; build_synthetic_intents reflects PROTOCOL_ROUTERS which no
        longer includes traderjoe_v2.
        """
        intents = build_synthetic_intents("traderjoe_v2", "SWAP", "avalanche")
        assert intents == []

    def test_protocol_not_on_chain_returns_empty(self):
        """Protocol without a router on this chain returns empty."""
        # aerodrome only has a router on base
        intents = build_synthetic_intents("aerodrome", "SWAP", "arbitrum")
        assert intents == []

    def test_non_swap_protocol_returns_empty(self):
        """Lending protocol should not produce SWAP intents."""
        intents = build_synthetic_intents("aave_v3", "SWAP", "arbitrum")
        assert intents == []

    def test_enso_excluded_from_swap(self):
        """Enso uses DELEGATECALL via the generator, not compilation-based discovery."""
        intents = build_synthetic_intents("enso", "SWAP", "arbitrum")
        assert intents == []


class TestLPIntents:
    """Test synthetic LP intent creation."""

    def test_lp_open_uniswap_v3(self):
        """LP_OPEN for uniswap_v3 should produce an LPOpenIntent."""
        intents = build_synthetic_intents("uniswap_v3", "LP_OPEN", "arbitrum")
        assert len(intents) == 1
        assert isinstance(intents[0], LPOpenIntent)

    def test_lp_close_uniswap_v3(self):
        """LP_CLOSE for uniswap_v3 should produce an LPCloseIntent."""
        intents = build_synthetic_intents("uniswap_v3", "LP_CLOSE", "arbitrum")
        assert len(intents) == 1
        assert isinstance(intents[0], LPCloseIntent)
        assert intents[0].position_id == "1"

    def test_lp_collect_fees(self):
        """LP_COLLECT_FEES should produce a CollectFeesIntent only for traderjoe_v2."""
        intents = build_synthetic_intents("traderjoe_v2", "LP_COLLECT_FEES", "avalanche")
        assert len(intents) == 1
        assert isinstance(intents[0], CollectFeesIntent)
        assert "/" in intents[0].pool  # pool format: "token0/token1"

    def test_lp_collect_fees_unsupported_protocol(self):
        """LP_COLLECT_FEES returns empty for protocols that don't support standalone collection."""
        intents = build_synthetic_intents("uniswap_v3", "LP_COLLECT_FEES", "arbitrum")
        assert intents == []

    def test_lp_close_aerodrome_position_format(self):
        """Aerodrome LP_CLOSE uses TOKEN0/TOKEN1/volatile format, not NFT ID."""
        intents = build_synthetic_intents("aerodrome", "LP_CLOSE", "base")
        assert len(intents) == 1
        assert isinstance(intents[0], LPCloseIntent)
        assert "volatile" in intents[0].position_id
        parts = intents[0].position_id.split("/")
        assert len(parts) == 3  # TOKEN0/TOKEN1/volatile

    def test_lp_open_no_manager_returns_empty(self):
        """LP protocol without a position manager on this chain returns empty."""
        # aerodrome LP manager is only on base
        intents = build_synthetic_intents("aerodrome", "LP_OPEN", "arbitrum")
        assert intents == []

    def test_lending_protocol_no_lp(self):
        """Lending protocol should not produce LP intents."""
        intents = build_synthetic_intents("aave_v3", "LP_OPEN", "arbitrum")
        assert intents == []


class TestLendingIntents:
    """Test synthetic lending intent creation."""

    def test_supply_aave_v3(self):
        """SUPPLY for aave_v3 should produce a SupplyIntent."""
        intents = build_synthetic_intents("aave_v3", "SUPPLY", "arbitrum")
        assert len(intents) == 1
        assert isinstance(intents[0], SupplyIntent)
        assert intents[0].protocol == "aave_v3"

    def test_withdraw_aave_v3(self):
        """WITHDRAW for aave_v3 should produce a WithdrawIntent."""
        intents = build_synthetic_intents("aave_v3", "WITHDRAW", "arbitrum")
        assert len(intents) == 1
        assert isinstance(intents[0], WithdrawIntent)

    def test_borrow_aave_v3(self):
        """BORROW for aave_v3 should produce a BorrowIntent with collateral."""
        intents = build_synthetic_intents("aave_v3", "BORROW", "arbitrum")
        assert len(intents) == 1
        assert isinstance(intents[0], BorrowIntent)
        assert intents[0].collateral_token is not None
        assert intents[0].borrow_token is not None

    def test_repay_aave_v3(self):
        """REPAY for aave_v3 should produce a RepayIntent."""
        intents = build_synthetic_intents("aave_v3", "REPAY", "arbitrum")
        assert len(intents) == 1
        assert isinstance(intents[0], RepayIntent)

    def test_swap_protocol_no_lending(self):
        """Swap protocol should not produce lending intents."""
        intents = build_synthetic_intents("uniswap_v3", "SUPPLY", "arbitrum")
        assert intents == []


class TestFlashLoanIntents:
    """Test synthetic flash loan intent creation."""

    def test_aave_flash_loan(self):
        intents = build_synthetic_intents("aave", "FLASH_LOAN", "arbitrum")
        assert len(intents) == 1
        assert isinstance(intents[0], FlashLoanIntent)
        assert intents[0].provider == "aave"
        assert len(intents[0].callback_intents) >= 1

    def test_balancer_flash_loan(self):
        intents = build_synthetic_intents("balancer", "FLASH_LOAN", "arbitrum")
        assert len(intents) == 1
        assert isinstance(intents[0], FlashLoanIntent)
        assert len(intents[0].callback_intents) >= 1

    def test_non_provider_returns_empty(self):
        intents = build_synthetic_intents("uniswap_v3", "FLASH_LOAN", "arbitrum")
        assert intents == []


class TestVaultIntents:
    """Test synthetic vault intent creation."""

    def test_vault_deposit_ethereum(self):
        intents = build_synthetic_intents("metamorpho", "VAULT_DEPOSIT", "ethereum")
        assert len(intents) == 1
        assert isinstance(intents[0], VaultDepositIntent)

    def test_vault_redeem_ethereum(self):
        intents = build_synthetic_intents("metamorpho", "VAULT_REDEEM", "ethereum")
        assert len(intents) == 1
        assert isinstance(intents[0], VaultRedeemIntent)

    def test_vault_unsupported_chain_returns_empty(self):
        """No MetaMorpho vault on arbitrum."""
        intents = build_synthetic_intents("metamorpho", "VAULT_DEPOSIT", "arbitrum")
        assert intents == []

    def test_non_vault_protocol_returns_empty(self):
        intents = build_synthetic_intents("uniswap_v3", "VAULT_DEPOSIT", "ethereum")
        assert intents == []


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_unknown_intent_type_returns_empty(self):
        """Invalid intent type string returns empty list."""
        intents = build_synthetic_intents("uniswap_v3", "NONEXISTENT", "arbitrum")
        assert intents == []

    def test_unknown_protocol_returns_empty(self):
        """Unknown protocol returns empty for any intent type."""
        intents = build_synthetic_intents("unknown_proto", "SWAP", "arbitrum")
        assert intents == []

    def test_unknown_chain_returns_empty(self):
        """Unknown chain has no routers, so returns empty."""
        intents = build_synthetic_intents("uniswap_v3", "SWAP", "unknown_chain")
        assert intents == []

    def test_case_insensitive_protocol(self):
        """Protocol name is lowercased internally."""
        intents = build_synthetic_intents("Uniswap_V3", "SWAP", "arbitrum")
        assert len(intents) == 1


class TestCompilationSuccess:
    """Verify that synthetic intents actually compile without errors.

    This is the key property: if a synthetic intent can't compile,
    it's useless for permission discovery.
    """

    @pytest.fixture
    def compiler(self):
        return IntentCompiler(
            chain="arbitrum",
            config=IntentCompilerConfig(
                allow_placeholder_prices=True,
                swap_pool_selection_mode="fixed",
                fixed_swap_fee_tier=3000,
            ),
        )

    def test_swap_compiles(self, compiler):
        intents = build_synthetic_intents("uniswap_v3", "SWAP", "arbitrum")
        assert len(intents) == 1
        result = compiler.compile(intents[0])
        assert result.status.value == "SUCCESS"
        assert len(result.transactions) > 0

    def test_lp_open_compiles(self, compiler):
        intents = build_synthetic_intents("uniswap_v3", "LP_OPEN", "arbitrum")
        assert len(intents) == 1
        result = compiler.compile(intents[0])
        assert result.status.value == "SUCCESS"
        assert len(result.transactions) > 0

    def test_lp_close_compiles_or_warns(self, compiler):
        """LP_CLOSE may fail in offline mode (needs RPC for position liquidity).

        The discovery module handles this gracefully by recording warnings.
        """
        intents = build_synthetic_intents("uniswap_v3", "LP_CLOSE", "arbitrum")
        assert len(intents) == 1
        result = compiler.compile(intents[0])
        # LP_CLOSE needs on-chain state, so it may fail without RPC.
        # What matters is that it doesn't crash.
        assert result.status.value in ("SUCCESS", "FAILED")

    def test_supply_compiles(self, compiler):
        intents = build_synthetic_intents("aave_v3", "SUPPLY", "arbitrum")
        assert len(intents) == 1
        result = compiler.compile(intents[0])
        assert result.status.value == "SUCCESS"
        assert len(result.transactions) > 0

    def test_borrow_compiles(self, compiler):
        intents = build_synthetic_intents("aave_v3", "BORROW", "arbitrum")
        assert len(intents) == 1
        result = compiler.compile(intents[0])
        assert result.status.value == "SUCCESS"

    def test_withdraw_compiles(self, compiler):
        intents = build_synthetic_intents("aave_v3", "WITHDRAW", "arbitrum")
        assert len(intents) == 1
        result = compiler.compile(intents[0])
        assert result.status.value == "SUCCESS"

    def test_repay_compiles(self, compiler):
        intents = build_synthetic_intents("aave_v3", "REPAY", "arbitrum")
        assert len(intents) == 1
        result = compiler.compile(intents[0])
        assert result.status.value == "SUCCESS"

    def test_collect_fees_compiles_or_warns(self):
        """LP_COLLECT_FEES for traderjoe_v2 on avalanche compiles or warns gracefully."""
        avalanche_compiler = IntentCompiler(
            chain="avalanche",
            config=IntentCompilerConfig(
                allow_placeholder_prices=True,
                swap_pool_selection_mode="fixed",
                fixed_swap_fee_tier=3000,
            ),
        )
        intents = build_synthetic_intents("traderjoe_v2", "LP_COLLECT_FEES", "avalanche")
        assert len(intents) == 1
        result = avalanche_compiler.compile(intents[0])
        # May fail without RPC - what matters is no crash
        assert result.status.value in ("SUCCESS", "FAILED")


class TestPermissionHints:
    """Test the PermissionHints system and convention-based discovery."""

    def test_default_hints(self):
        """Unknown protocol gets default PermissionHints."""
        hints = get_permission_hints("unknown_protocol_xyz")
        assert hints.synthetic_position_id == "1"
        assert hints.supports_standalone_fee_collection is False
        assert hints.selector_labels == {}
        assert hints.synthetic_market_id is None
        assert hints.synthetic_fee_tier == {}
        assert hints.static_permissions == {}

    def test_aerodrome_hints_loaded(self):
        """Aerodrome hints provide custom position_id format."""
        hints = get_permission_hints("aerodrome")
        assert "{token0}" in hints.synthetic_position_id
        assert "{token1}" in hints.synthetic_position_id
        assert "volatile" in hints.synthetic_position_id

    def test_traderjoe_v2_hints_loaded(self):
        """TraderJoe V2 hints enable standalone fee collection."""
        hints = get_permission_hints("traderjoe_v2")
        assert hints.supports_standalone_fee_collection is True

    def test_gmx_v2_hints_loaded(self):
        """GMX V2 hints provide multicall selector label."""
        hints = get_permission_hints("gmx_v2")
        assert "0xac9650d8" in hints.selector_labels
        assert "multicall" in hints.selector_labels["0xac9650d8"]

    def test_aerodrome_selector_labels(self):
        """Aerodrome hints include protocol-specific selector labels."""
        hints = get_permission_hints("aerodrome")
        assert len(hints.selector_labels) == 4
        assert "0xa026383e" in hints.selector_labels
        assert "0x5a47ddc3" in hints.selector_labels

    def test_morpho_blue_hints_loaded(self):
        """Morpho Blue hints provide synthetic market_id."""
        hints = get_permission_hints("morpho_blue")
        assert hints.synthetic_market_id is not None
        assert hints.synthetic_market_id.startswith("0x")

    def test_uniswap_v3_mantle_fee_tier(self):
        """Uniswap V3 hints provide fee tier override for mantle."""
        hints = get_permission_hints("uniswap_v3")
        assert hints.synthetic_fee_tier.get("mantle") == 500

    def test_metamorpho_static_permissions(self):
        """MetaMorpho hints provide static permissions for vault chains."""
        hints = get_permission_hints("metamorpho")
        assert "ethereum" in hints.static_permissions
        assert "base" in hints.static_permissions
        eth_perms = hints.static_permissions["ethereum"]
        assert len(eth_perms) == 2  # approve + vault
        # Vault entry should have deposit and redeem selectors
        vault_entry = next(p for p in eth_perms if "Vault" in p.label)
        assert "0x6e553f65" in vault_entry.selectors  # deposit
        assert "0xba087652" in vault_entry.selectors  # redeem

    def test_hints_are_frozen(self):
        """PermissionHints should be immutable (frozen dataclass)."""
        hints = get_permission_hints("aerodrome")
        with pytest.raises(AttributeError):
            hints.synthetic_position_id = "changed"
