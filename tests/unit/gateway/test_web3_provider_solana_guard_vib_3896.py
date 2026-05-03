"""Regression guards for VIB-3896 (QA Tier-3 follow-up).

PR #2005 (Tier-3 bundle, VIB-3816) added a defensive ``"solana": "SOL"`` entry
to ``NATIVE_TOKEN_SYMBOLS`` to stop the immediate
``DataSourceUnavailable: Cannot resolve token 'ETH' on solana`` crash, but the
underlying smell — instantiating an EVM-only ``Web3BalanceProvider`` with
``chain='solana'`` — was left in place. VIB-3896 closes the loop by:

1. Raising :class:`NonEvmChainError` from ``Web3BalanceProvider.__init__`` so
   any future caller fails fast at construction instead of getting a broken
   provider that crashes on the first RPC call.
2. Dropping non-EVM chains from ``MultiChainWeb3BalanceProvider`` at
   construction so a mixed EVM+Solana ``rpc_urls`` map still produces a valid
   (EVM-only) multichain provider.
3. Routing ``strategy_runner._handle_execution_failure`` away from the EVM
   revert-diagnostic path for non-EVM chains.
"""

from __future__ import annotations

import pytest

from almanak.gateway.data.balance.multichain_provider import (
    MultiChainWeb3BalanceProvider,
)
from almanak.gateway.data.balance.web3_provider import (
    NonEvmChainError,
    Web3BalanceProvider,
    _reject_non_evm_chain,
)

EVM_TEST_WALLET = "0x000000000000000000000000000000000000dEaD"


class TestRejectNonEvmChainHelper:
    def test_evm_chain_passes(self) -> None:
        for chain in ("arbitrum", "ethereum", "base", "POLYGON", "Avalanche"):
            _reject_non_evm_chain(chain)

    def test_solana_raises(self) -> None:
        with pytest.raises(NonEvmChainError) as exc_info:
            _reject_non_evm_chain("solana")
        assert exc_info.value.chain == "solana"
        assert exc_info.value.family == "SOLANA"

    def test_solana_uppercase_raises(self) -> None:
        with pytest.raises(NonEvmChainError):
            _reject_non_evm_chain("SOLANA")

    def test_unknown_chain_does_not_raise(self) -> None:
        _reject_non_evm_chain("aptos")  # not yet registered → no decision


class TestWeb3BalanceProviderConstructionGuard:
    def test_arbitrum_construction_succeeds(self) -> None:
        provider = Web3BalanceProvider(
            rpc_url="http://localhost:8545",
            wallet_address=EVM_TEST_WALLET,
            chain="arbitrum",
        )
        assert provider._chain == "arbitrum"

    def test_solana_construction_raises(self) -> None:
        with pytest.raises(NonEvmChainError) as exc_info:
            Web3BalanceProvider(
                rpc_url="https://api.mainnet-beta.solana.com",
                wallet_address=EVM_TEST_WALLET,
                chain="solana",
            )
        msg = str(exc_info.value)
        assert "solana" in msg.lower()
        assert "evm" in msg.lower()

    def test_solana_guard_fires_before_address_checksum(self) -> None:
        """Even with an invalid wallet address, the chain guard fires first."""
        with pytest.raises(NonEvmChainError):
            Web3BalanceProvider(
                rpc_url="https://api.mainnet-beta.solana.com",
                wallet_address="not-an-evm-address",
                chain="solana",
            )


class TestMultiChainWeb3BalanceProviderSkipsSolana:
    def test_mixed_chains_drops_solana(self) -> None:
        provider = MultiChainWeb3BalanceProvider(
            rpc_urls={
                "arbitrum": "http://localhost:8545",
                "solana": "https://api.mainnet-beta.solana.com",
                "base": "http://localhost:8546",
            },
            wallet_address=EVM_TEST_WALLET,
        )
        assert "solana" not in provider._rpc_urls
        assert "arbitrum" in provider._rpc_urls
        assert "base" in provider._rpc_urls

    def test_solana_only_yields_empty_provider(self) -> None:
        provider = MultiChainWeb3BalanceProvider(
            rpc_urls={"solana": "https://api.mainnet-beta.solana.com"},
            wallet_address=EVM_TEST_WALLET,
        )
        assert provider._rpc_urls == {}

    def test_evm_only_unchanged(self) -> None:
        provider = MultiChainWeb3BalanceProvider(
            rpc_urls={
                "arbitrum": "http://localhost:8545",
                "BASE": "http://localhost:8546",
            },
            wallet_address=EVM_TEST_WALLET,
        )
        assert set(provider._rpc_urls) == {"arbitrum", "base"}
