"""Regression tests for VIB-3961 — IntentCompiler wallet normalization.

The compiler is the single boundary where strategy-supplied wallet strings
enter the framework. Storing the value verbatim leaks the case-validation
burden into every connector SDK and crashes when a wallet's lowercase form
is not EIP-55 valid (prod 2026-05-04, AerodromeSlipstreamUsdcCbbtcLpStrategy).

Contract:
- EVM-shaped addresses (``0x``-prefixed) are checksummed at construction.
- Solana base58 pubkeys (no ``0x`` prefix) pass through unchanged.
- ``chain_wallets`` values are normalized the same way.
- A malformed hex string raises ``ValueError`` (fail-fast at the boundary).
"""

from __future__ import annotations

import pytest
from web3 import Web3

from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.compiler_models import IntentCompilerConfig

# The 2026-05-04 prod wallet — lowercase form is NOT EIP-55 valid.
NON_CHECKSUM_WALLET = "0xca69825e381929621c8b614c9042b85a3f446947"
CHECKSUMMED_WALLET = Web3.to_checksum_address(NON_CHECKSUM_WALLET)

# A valid Solana base58 pubkey (32-44 chars, no ``0x``).
SOLANA_PUBKEY = "5tzFkiKscXHK5ZXCGbXbxpU5pYFKkUkudKKfUe6tFEsB"


@pytest.fixture
def placeholder_config() -> IntentCompilerConfig:
    return IntentCompilerConfig(allow_placeholder_prices=True)


class TestWalletAddressNormalization:
    """``IntentCompiler.wallet_address`` must be checksummed on EVM chains."""

    def test_lowercase_evm_wallet_is_checksummed(
        self, placeholder_config: IntentCompilerConfig
    ) -> None:
        compiler = IntentCompiler(
            chain="base",
            wallet_address=NON_CHECKSUM_WALLET,
            config=placeholder_config,
        )

        assert compiler.wallet_address == CHECKSUMMED_WALLET
        # Sanity: the input and output forms genuinely differ for this wallet.
        assert compiler.wallet_address != NON_CHECKSUM_WALLET

    def test_already_checksummed_wallet_is_idempotent(
        self, placeholder_config: IntentCompilerConfig
    ) -> None:
        compiler = IntentCompiler(
            chain="base",
            wallet_address=CHECKSUMMED_WALLET,
            config=placeholder_config,
        )
        assert compiler.wallet_address == CHECKSUMMED_WALLET

    def test_solana_pubkey_passes_through(
        self, placeholder_config: IntentCompilerConfig
    ) -> None:
        """Base58 Solana pubkeys do not start with ``0x`` and must not be touched."""
        compiler = IntentCompiler(
            chain="solana",
            wallet_address=SOLANA_PUBKEY,
            config=placeholder_config,
        )
        assert compiler.wallet_address == SOLANA_PUBKEY

    def test_malformed_hex_address_raises(
        self, placeholder_config: IntentCompilerConfig
    ) -> None:
        """Bad hex shapes must fail loudly at the boundary, not propagate."""
        with pytest.raises(ValueError):
            IntentCompiler(
                chain="base",
                wallet_address="0xINVALID",
                config=placeholder_config,
            )

    def test_chain_wallets_values_are_checksummed(
        self, placeholder_config: IntentCompilerConfig
    ) -> None:
        """Cross-chain wallet registry entries get the same normalization."""
        compiler = IntentCompiler(
            chain="base",
            wallet_address=NON_CHECKSUM_WALLET,
            config=placeholder_config,
            chain_wallets={
                "base": NON_CHECKSUM_WALLET,
                "arbitrum": NON_CHECKSUM_WALLET.upper().replace("0X", "0x"),
            },
        )

        assert compiler._chain_wallets is not None
        assert compiler._chain_wallets["base"] == CHECKSUMMED_WALLET
        assert compiler._chain_wallets["arbitrum"] == CHECKSUMMED_WALLET

    def test_default_zero_address_is_accepted(
        self, placeholder_config: IntentCompilerConfig
    ) -> None:
        """The placeholder zero wallet must round-trip cleanly through the normalizer."""
        zero = "0x0000000000000000000000000000000000000000"
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address=zero,
            config=placeholder_config,
        )
        assert compiler.wallet_address == Web3.to_checksum_address(zero)

    def test_chain_wallets_keys_are_lowercased(
        self, placeholder_config: IntentCompilerConfig
    ) -> None:
        """Mixed-case chain keys must match the lowercase lookup at ``_resolve_dest_wallet``.

        Without normalization, ``{"Base": ...}`` would silently fall through to
        ``self.wallet_address`` and could misroute a bridge destination wallet.
        """
        compiler = IntentCompiler(
            chain="base",
            wallet_address=NON_CHECKSUM_WALLET,
            config=placeholder_config,
            chain_wallets={
                "Base": NON_CHECKSUM_WALLET,
                " ARBITRUM ": NON_CHECKSUM_WALLET,
            },
        )

        assert compiler._chain_wallets is not None
        # Keys are trimmed and lowercased.
        assert "base" in compiler._chain_wallets
        assert "arbitrum" in compiler._chain_wallets
        # And the cross-chain lookup (which calls ``dest_chain.lower()``) resolves.
        assert compiler._resolve_dest_wallet("Base") == CHECKSUMMED_WALLET
        assert compiler._resolve_dest_wallet("arbitrum") == CHECKSUMMED_WALLET
