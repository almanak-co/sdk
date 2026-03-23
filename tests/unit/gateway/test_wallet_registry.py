"""Tests for WalletRegistry and PlatformWalletRegistry.

Covers acceptance scenarios: A34, A37, A41, A42.
Requires almanak-platform-plugins to be installed (skip if not available).
"""

import os
from unittest.mock import patch

import pytest

pytest.importorskip("almanak_platform", reason="almanak-platform-plugins not installed")

from almanak_platform.wallets.registry import (  # noqa: E402
    PlatformWalletRegistry,
    WalletRegistry,
    _normalize_registry_chain,
)
from almanak_platform.wallets.types import WalletFamily, WalletType  # noqa: E402


class TestNormalizeRegistryChain:
    """A34: Chain alias normalization."""

    def test_bnb_normalizes_to_bsc(self):
        assert _normalize_registry_chain("bnb") == "bsc"

    def test_eth_normalizes_to_ethereum(self):
        assert _normalize_registry_chain("eth") == "ethereum"

    def test_arb_normalizes_to_arbitrum(self):
        assert _normalize_registry_chain("arb") == "arbitrum"

    def test_solana_passthrough(self):
        """Solana is not in EVM alias map, accepted as-is."""
        assert _normalize_registry_chain("solana") == "solana"

    def test_uppercase_normalized(self):
        assert _normalize_registry_chain("BNB") == "bsc"

    def test_whitespace_stripped(self):
        assert _normalize_registry_chain("  arbitrum  ") == "arbitrum"


class TestWalletRegistryFromRaw:
    """A37: Duplicate chain after normalization."""

    def test_duplicate_chain_after_normalization_raises(self):
        with pytest.raises(ValueError, match="Duplicate chain"):
            WalletRegistry.from_raw({
                "bnb": {
                    "wallet_address": "0xABC",
                    "type": "zodiac",
                    "eoa_address": "0xEOA",
                    "zodiac_roles_address": "0xZod",
                },
                "bsc": {
                    "wallet_address": "0xABC",
                    "type": "zodiac",
                    "eoa_address": "0xEOA",
                    "zodiac_roles_address": "0xZod",
                },
            })

    def test_solana_chain_parses_ok(self):
        """Solana chain is parseable (not rejected by registry)."""
        registry = WalletRegistry.from_raw({
            "solana": {"wallet_address": "SolAddr123", "type": "squads"},
        })
        wallet = registry.resolve("solana")
        assert wallet.family == WalletFamily.SOLANA
        assert wallet.kind == WalletType.SQUADS

    def test_resolve_unknown_chain_raises(self):
        registry = WalletRegistry.from_raw({
            "arbitrum": {
                "wallet_address": "0xABC",
                "type": "zodiac",
                "eoa_address": "0xEOA",
                "zodiac_roles_address": "0xZod",
            },
        })
        with pytest.raises(KeyError, match="No wallet configured"):
            registry.resolve("base")

    def test_is_uniform_true(self):
        registry = WalletRegistry.from_raw({
            "arbitrum": {
                "wallet_address": "0xABC",
                "type": "zodiac",
                "eoa_address": "0xEOA",
                "zodiac_roles_address": "0xZod",
            },
            "base": {
                "wallet_address": "0xABC",
                "type": "zodiac",
                "eoa_address": "0xEOA",
                "zodiac_roles_address": "0xZod",
            },
        })
        assert registry.is_uniform()

    def test_is_uniform_false(self):
        registry = WalletRegistry.from_raw({
            "arbitrum": {
                "wallet_address": "0xABC",
                "type": "zodiac",
                "eoa_address": "0xEOA",
                "zodiac_roles_address": "0xZod",
            },
            "base": {
                "wallet_address": "0xDEF",
                "type": "zodiac",
                "eoa_address": "0xEOA2",
                "zodiac_roles_address": "0xZod2",
            },
        })
        assert not registry.is_uniform()


class TestPlatformWalletRegistryFromEnv:
    """A41, A42: Environment variable parsing."""

    def test_non_uniform_direct_wallets_rejected(self):
        """A41: Non-uniform DIRECT wallets raise ValueError."""
        wallets_json = (
            '{"arbitrum": {"wallet_address": "0xABC", "type": "direct"}, '
            '"base": {"wallet_address": "0xDEF", "type": "direct"}}'
        )
        with patch.dict(os.environ, {"ALMANAK_GATEWAY_WALLETS": wallets_json}, clear=False):
            with pytest.raises(ValueError, match="Non-uniform DIRECT"):
                PlatformWalletRegistry.from_env()

    def test_direct_address_mismatch_with_private_key_rejected(self):
        """A42: DIRECT address must match derived key."""
        # 0x0000...dead is definitely not derived from this key
        wallets_json = (
            '{"arbitrum": {"wallet_address": "0x000000000000000000000000000000000000dead", "type": "direct"}}'
        )
        # Use a known test private key
        test_key = "0x" + "a" * 64
        with patch.dict(
            os.environ,
            {
                "ALMANAK_GATEWAY_WALLETS": wallets_json,
                "ALMANAK_PRIVATE_KEY": test_key,
            },
            clear=False,
        ):
            with pytest.raises(ValueError, match="does not match"):
                PlatformWalletRegistry.from_env()

    def test_from_env_with_safe_wallet_address_legacy(self):
        """Legacy fallback from SAFE_WALLET_ADDRESS."""
        env = {
            "SAFE_WALLET_ADDRESS": "0xSafe123",
            "ALMANAK_GATEWAY_SAFE_MODE": "zodiac",
            "EOA_ADDRESS": "0xEOA456",
            "ZODIAC_ROLES_ADDRESS": "0xZod789",
        }
        # Clear ALMANAK_GATEWAY_WALLETS to ensure legacy path
        env_clear = {"ALMANAK_GATEWAY_WALLETS": ""}
        with patch.dict(os.environ, {**env, **env_clear}, clear=False):
            # Remove the key entirely
            os.environ.pop("ALMANAK_GATEWAY_WALLETS", None)
            registry = PlatformWalletRegistry.from_env(default_chains=["arbitrum", "base"])

        assert sorted(registry.all_chains()) == ["arbitrum", "base"]
        arb = registry.resolve("arbitrum")
        assert arb.account_address == "0xSafe123"
        assert arb.kind == WalletType.ZODIAC
        assert arb.config["eoa_address"] == "0xEOA456"

    def test_from_env_new_takes_precedence_over_legacy(self):
        """ALMANAK_GATEWAY_WALLETS takes precedence over SAFE_WALLET_ADDRESS."""
        wallets_json = (
            '{"arbitrum": {"wallet_address": "0xNew", "type": "zodiac", '
            '"eoa_address": "0xEOA", "zodiac_roles_address": "0xZod"}}'
        )
        env = {
            "ALMANAK_GATEWAY_WALLETS": wallets_json,
            "SAFE_WALLET_ADDRESS": "0xOld",
        }
        with patch.dict(os.environ, env, clear=False):
            registry = PlatformWalletRegistry.from_env()

        arb = registry.resolve("arbitrum")
        assert arb.account_address == "0xNew"

    def test_lazy_materialization_from_legacy(self):
        """Legacy default materializes wallet on resolve()."""
        env = {
            "SAFE_WALLET_ADDRESS": "0xSafe",
            "ALMANAK_GATEWAY_SAFE_MODE": "direct",
        }
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("ALMANAK_GATEWAY_WALLETS", None)
            registry = PlatformWalletRegistry.from_env()  # No default_chains

        # Should lazily create wallet for any requested chain
        wallet = registry.resolve("arbitrum")
        assert wallet.account_address == "0xSafe"
        assert wallet.chain == "arbitrum"
        assert wallet.kind == WalletType.DIRECT
