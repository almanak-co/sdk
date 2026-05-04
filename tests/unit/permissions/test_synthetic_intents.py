"""Tests for the synthetic intent factory."""

import pytest

from almanak.framework.intents.compiler import (
    LENDING_POOL_ADDRESSES,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import (
    BorrowIntent,
    CollectFeesIntent,
    FlashLoanIntent,
    LPCloseIntent,
    LPOpenIntent,
    RepayIntent,
    SupplyIntent,
    SwapIntent,
    VaultDepositIntent,
    VaultRedeemIntent,
    WithdrawIntent,
)
from almanak.framework.permissions.hints import get_permission_hints
from almanak.framework.permissions.synthetic_intents import build_synthetic_intents


class TestSwapIntents:
    """Test synthetic SWAP intent creation."""

    def test_uniswap_v3_arbitrum(self):
        """uniswap_v3 on arbitrum produces ERC20 + native-input SwapIntents.

        V3-style SwapRouter02 wraps the chain native via msg.value (no approve,
        single value-bearing tx). The second synthetic flips ``send_allowed=True``
        on the router target so Zodiac authorises native-in swaps under
        execTransactionWithRole.
        """
        intents = build_synthetic_intents("uniswap_v3", "SWAP", "arbitrum")
        assert len(intents) == 2
        assert all(isinstance(i, SwapIntent) for i in intents)
        assert all(i.protocol == "uniswap_v3" for i in intents)
        assert all(i.chain == "arbitrum" for i in intents)
        # First synthetic: ERC20-in (USDC → WETH)
        assert intents[0].from_token.startswith("0x")
        # Second synthetic: native-in (ETH → USDC). The compiler sets value > 0
        # for native-from, which the discovery loop translates to send_allowed.
        assert intents[1].from_token == "ETH"

    def test_aerodrome_base(self):
        """aerodrome on base should produce a SwapIntent.

        Aerodrome is a Solidly fork (not in ``_NATIVE_IN_SWAP_PROTOCOLS``) so
        only the ERC20 synthetic is emitted.
        """
        intents = build_synthetic_intents("aerodrome", "SWAP", "base")
        assert len(intents) == 1
        assert isinstance(intents[0], SwapIntent)

    def test_traderjoe_v2_avalanche(self):
        """traderjoe_v2 swap on avalanche produces a SwapIntent (issue #1841).

        TJv2 has a dedicated compile path (``_compile_swap_traderjoe_v2``, VIB-1928)
        so its LBRouter address lives in ``LP_POSITION_MANAGERS`` rather than
        ``PROTOCOL_ROUTERS``. ``_build_swap_intents`` used to skip TJv2 because of
        the PROTOCOL_ROUTERS check, producing a manifest that omitted the
        ``swapExactTokensForTokens`` selector (0x2a443fae) and caused Zodiac
        authorisation to revert on-chain. TJv2 is now exempt from that check so
        the real compile path runs.
        """
        intents = build_synthetic_intents("traderjoe_v2", "SWAP", "avalanche")
        assert len(intents) == 1
        assert isinstance(intents[0], SwapIntent)
        assert intents[0].protocol == "traderjoe_v2"
        assert intents[0].chain == "avalanche"

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


class TestSparkLendingRegistration:
    """Regression tests for spark pool registration in LENDING_POOL_ADDRESSES.

    Spark is an Aave V3 fork on Ethereum with a Spark-specific deployment.
    The synthetic intent builders (_build_supply/withdraw/borrow/repay_intents)
    short-circuit to ``[]`` when a protocol is missing from
    ``LENDING_POOL_ADDRESSES[chain]`` (unless it's on the registry-exempt
    allowlist: morpho_blue, compound_v3). Spark isn't registry-exempt -- it has
    a conventional Pool contract -- so registering its address is the gate
    that unblocks synthetic discovery.

    Note on compiler dispatch: spark does NOT go through the
    AAVE_COMPATIBLE_PROTOCOLS branch. ``compiler_lending.py`` has dedicated
    ``_compile_{supply,withdraw,borrow,repay}_spark`` functions matched by an
    explicit ``if protocol_lower == "spark"`` check after the Aave-compatible
    branch. So pool registration alone (no adapter-registry change) is
    sufficient to route spark end-to-end.
    """

    def test_spark_pool_registered_on_ethereum(self):
        """Spark's Ethereum pool must be in LENDING_POOL_ADDRESSES."""
        assert "spark" in LENDING_POOL_ADDRESSES["ethereum"], (
            "Spark missing from LENDING_POOL_ADDRESSES[ethereum]; "
            "synthetic intent discovery will short-circuit."
        )
        # Pin the exact deployment address so a wrong 42-char hex still fails.
        addr = LENDING_POOL_ADDRESSES["ethereum"]["spark"]
        assert addr == "0xC13e21B648A5Ee794902342038FF3aDAB66BE987", (
            f"Unexpected spark pool address on ethereum: {addr}. "
            "Expected the canonical Spark LendingPool deployment."
        )

    def test_supply_spark_ethereum(self):
        """SUPPLY for spark on ethereum produces a SupplyIntent."""
        intents = build_synthetic_intents("spark", "SUPPLY", "ethereum")
        assert len(intents) == 1
        assert isinstance(intents[0], SupplyIntent)
        assert intents[0].protocol == "spark"
        assert intents[0].chain == "ethereum"

    def test_withdraw_spark_ethereum(self):
        """WITHDRAW for spark on ethereum produces a WithdrawIntent."""
        intents = build_synthetic_intents("spark", "WITHDRAW", "ethereum")
        assert len(intents) == 1
        assert isinstance(intents[0], WithdrawIntent)
        assert intents[0].protocol == "spark"

    def test_borrow_spark_ethereum(self):
        """BORROW for spark on ethereum produces a BorrowIntent."""
        intents = build_synthetic_intents("spark", "BORROW", "ethereum")
        assert len(intents) == 1
        assert isinstance(intents[0], BorrowIntent)
        assert intents[0].protocol == "spark"
        assert intents[0].collateral_token is not None
        assert intents[0].borrow_token is not None

    def test_repay_spark_ethereum(self):
        """REPAY for spark on ethereum produces a RepayIntent."""
        intents = build_synthetic_intents("spark", "REPAY", "ethereum")
        assert len(intents) == 1
        assert isinstance(intents[0], RepayIntent)
        assert intents[0].protocol == "spark"

    def test_spark_off_ethereum_returns_empty(self):
        """Spark is ethereum-only in the registry; arbitrum has no entry so
        synthetic discovery must short-circuit. Guards against accidentally
        leaking a non-deployed address to other chains."""
        intents = build_synthetic_intents("spark", "SUPPLY", "arbitrum")
        assert intents == []


class TestRadiantV2LendingRegistration:
    """Regression tests for radiant_v2 pool registration in
    ``LENDING_POOL_ADDRESSES``.

    Radiant V2 is an Aave V2 fork that was deployed on both Ethereum and
    Arbitrum. After the October 2024 attack, the Arbitrum LendingPool proxy
    was reduced to a stub implementation and the framework dropped the
    Arbitrum entry from ``LENDING_POOL_ADDRESSES`` (issues #1842 / #1847 /
    #1889). These tests pin the resulting contract: synthetic intent
    discovery must yield ``[]`` for every lending intent type on Arbitrum,
    while Ethereum (the protocol's only supported chain) keeps producing
    valid intents.
    """

    @pytest.mark.parametrize("intent_type", ["SUPPLY", "WITHDRAW", "BORROW", "REPAY"])
    def test_radiant_v2_off_arbitrum_returns_empty(self, intent_type):
        """Regression guard for issues #1842 / #1847 / #1889.

        The Radiant V2 LendingPool proxy on Arbitrum was reduced to a stub
        implementation after the October 2024 attack. ``LENDING_POOL_ADDRESSES``
        no longer registers ``radiant_v2`` under ``arbitrum``, so synthetic
        intent discovery must short-circuit for every lending intent type. If
        a future contributor silently re-adds the entry without acknowledging
        that the pool is permanently dead, the manifest generator would emit
        targets pointing at a stub — Zodiac would authorise the selectors and
        the on-chain call would still revert mid-bundle.
        """
        intents = build_synthetic_intents("radiant_v2", intent_type, "arbitrum")
        assert intents == []

    def test_radiant_v2_ethereum_supply_still_works(self):
        """Sister guard to ``test_radiant_v2_off_arbitrum_returns_empty`` —
        confirm we did not accidentally delete the *Ethereum* registration
        when scoping out Arbitrum. Radiant V2 on Ethereum is the protocol's
        only supported chain.
        """
        intents = build_synthetic_intents("radiant_v2", "SUPPLY", "ethereum")
        assert len(intents) == 1
        assert isinstance(intents[0], SupplyIntent)
        assert intents[0].protocol == "radiant_v2"
        assert intents[0].chain == "ethereum"


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
        """Protocol name is lowercased internally.

        uniswap_v3 emits two SWAP synthetics (ERC20 + native-in); the case-
        insensitive lookup must produce the same shape as the canonical key.
        """
        intents = build_synthetic_intents("Uniswap_V3", "SWAP", "arbitrum")
        assert len(intents) == 2
        assert all(isinstance(i, SwapIntent) for i in intents)


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
        # Two synthetics for V3-style routers: ERC20-in and native-in.
        # Both must compile so the manifest discovers both code paths.
        assert len(intents) == 2
        for intent in intents:
            result = compiler.compile(intent)
            assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
            assert len(result.transactions) > 0
        # The native-in synthetic must produce a value-bearing tx so the
        # discovery loop sets ``send_allowed=True`` on the router target.
        native_result = compiler.compile(intents[1])
        assert any(tx.value > 0 for tx in native_result.transactions), (
            "Native-in synthetic must produce at least one value-bearing tx"
        )

    def test_lp_open_compiles(self, compiler):
        intents = build_synthetic_intents("uniswap_v3", "LP_OPEN", "arbitrum")
        assert len(intents) == 1
        result = compiler.compile(intents[0])
        assert result.status.value == "SUCCESS"
        assert len(result.transactions) > 0

    def test_lp_close_compiles_in_discovery_mode(self):
        """LP_CLOSE must compile successfully in permission-discovery mode.

        VIB-1846 regression: previously the Uniswap-V3-style LP_CLOSE body
        required an RPC-backed liquidity query and returned FAILED offline,
        leaving the NonfungiblePositionManager selectors unauthorised in
        the generated manifest. ``permission_discovery=True`` (set by
        ``discover_permissions``) now substitutes a synthetic liquidity so
        the full ``decreaseLiquidity + collect + burn`` flow is emitted.
        """
        from almanak.framework.intents.compiler import (
            NFT_POSITION_BURN_SELECTOR,
            NFT_POSITION_COLLECT_SELECTOR,
            NFT_POSITION_DECREASE_SELECTOR,
        )

        compiler = IntentCompiler(
            chain="arbitrum",
            config=IntentCompilerConfig(
                allow_placeholder_prices=True,
                swap_pool_selection_mode="fixed",
                fixed_swap_fee_tier=3000,
                permission_discovery=True,
            ),
        )
        intents = build_synthetic_intents("uniswap_v3", "LP_CLOSE", "arbitrum")
        assert len(intents) == 1
        result = compiler.compile(intents[0])
        assert result.status.value == "SUCCESS", result.error
        selectors = {tx.data[:10] for tx in result.transactions if tx.data}
        assert {
            NFT_POSITION_DECREASE_SELECTOR,
            NFT_POSITION_COLLECT_SELECTOR,
            NFT_POSITION_BURN_SELECTOR,
        }.issubset(selectors), f"Missing NPM selectors in compiled LP_CLOSE: {selectors}"

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
