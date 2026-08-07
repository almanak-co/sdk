"""ALM-3186 — the Enso bridge state provider must never fabricate a balance.

Two defects are pinned here, each with a negative control that fails when the
fix is reverted:

1. **``decimals = 6 if symbol == "USDC" else 18``.** Every other 6-decimal token
   on the bridge path (USDT, USDC.e, USDbC, …) was scaled by ``10**18`` — a
   10^12 over-statement of the destination balance, in exactly the direction
   that declares a bridge complete early.

2. **A gateway response with no balance fields returned ``0``.** Bridge
   completion is a difference of two balance reads, so a fabricated zero on the
   snapshot side makes pre-existing destination funds look like bridge arrival:
   the transfer is declared complete before anything crossed and the strategy
   spends inventory it does not have. Empty is UNMEASURED, not zero.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.framework.execution.enso_state_provider import (
    TOKEN_ADDRESSES,
    BridgeBalanceUnavailableError,
    EnsoStateProvider,
)

WALLET = "0x" + "11" * 20


def _balance_response(*, raw_balance: str = "", balance: str = "", decimals: int = 0) -> SimpleNamespace:
    """A stand-in for ``gateway_pb2.BalanceResponse`` with proto3 defaults."""
    return SimpleNamespace(raw_balance=raw_balance, balance=balance, decimals=decimals)


def _provider_with_gateway(response: SimpleNamespace) -> EnsoStateProvider:
    gateway = SimpleNamespace(market=SimpleNamespace(GetBalance=Mock(return_value=response)))
    return EnsoStateProvider(rpc_urls={}, wallet_address=WALLET, gateway_client=gateway)


class TestMissingBalanceIsUnmeasuredNotZero:
    """Defect 2 — the ``return 0`` fallback."""

    def test_empty_response_raises_instead_of_reporting_zero(self):
        """Negative control: reverted, this returns ``0`` and the raise never fires."""
        provider = _provider_with_gateway(_balance_response())

        with pytest.raises(BridgeBalanceUnavailableError, match="no balance"):
            provider._get_token_balance_via_gateway("arbitrum", "USDC")

    def test_a_genuine_zero_balance_is_still_zero(self):
        """``"0"`` is a MEASURED zero and must keep flowing through unchanged.

        This is the other half of Empty != Zero: the fix must not turn a real
        zero balance into an error, or ``register_bridge_transfer`` would fail
        for the ordinary case of an empty destination wallet.
        """
        provider = _provider_with_gateway(_balance_response(raw_balance="0"))

        assert provider._get_token_balance_via_gateway("arbitrum", "USDC") == 0

    def test_register_bridge_transfer_refuses_an_unmeasured_snapshot(self):
        """The premature-completion path: registration must not snapshot a fake 0.

        With the old ``return 0``, a wallet already holding 1 WETH on the
        destination would register ``initial_balance=0`` and the very first poll
        would compute ``increase = 1 WETH`` and declare the bridge complete.
        """
        provider = _provider_with_gateway(_balance_response())

        with pytest.raises(BridgeBalanceUnavailableError):
            provider.register_bridge_transfer(
                source_chain="base",
                destination_chain="arbitrum",
                source_tx_hash="0x" + "ab" * 32,
                token_symbol="WETH",
                expected_amount=10**18,
            )
        assert provider._pending_transfers == {}


class TestHumanReadableBalanceScaling:
    """Defect 1 — the USDC-else-18 decimals guess on the ``balance`` fallback."""

    def test_gateway_supplied_decimals_are_used(self):
        """The same read that produced the balance also states its decimals."""
        provider = _provider_with_gateway(_balance_response(balance="1.5", decimals=6))

        assert provider._get_token_balance_via_gateway("arbitrum", "USDT") == 1_500_000

    def test_non_usdc_six_decimal_token_is_not_scaled_by_1e18(self):
        """Negative control for the guess: USDT is 6 decimals, not 18.

        Reverted, this returns ``1.5 * 10**18`` — a 10^12 over-statement of the
        destination balance.
        """
        provider = _provider_with_gateway(_balance_response(balance="1.5"))

        assert provider._get_token_balance_via_gateway("arbitrum", "USDT") == 1_500_000

    def test_weth_still_scales_by_1e18(self):
        """Control arm: an 18-decimal token is unchanged by the fix."""
        provider = _provider_with_gateway(_balance_response(balance="1.5"))

        assert provider._get_token_balance_via_gateway("arbitrum", "WETH") == 1_500_000_000_000_000_000

    def test_native_eth_uses_the_chain_descriptor(self):
        """``ETH`` is the sentinel entry in ``TOKEN_ADDRESSES``, not an ERC-20."""
        provider = _provider_with_gateway(_balance_response(balance="2"))

        assert provider._get_token_balance_via_gateway("arbitrum", "ETH") == 2 * 10**18

    def test_unresolvable_token_raises_instead_of_defaulting_to_18(self):
        """A token the registry does not know has UNKNOWN decimals, not 18."""
        provider = _provider_with_gateway(_balance_response(balance="1"))

        with pytest.raises(BridgeBalanceUnavailableError, match="Refusing to guess"):
            provider._get_token_balance_via_gateway("arbitrum", "NOT_A_REAL_TOKEN")


class TestGetBalanceDecimals:
    """``get_balance`` carried the same USDC-else-18 guess."""

    @staticmethod
    def _provider(balance_wei: int) -> EnsoStateProvider:
        provider = EnsoStateProvider(rpc_urls={}, wallet_address=WALLET)
        provider._get_token_balance = Mock(return_value=balance_wei)
        return provider

    def test_usdt_is_six_decimals(self):
        """Negative control: reverted, this reports ``0.0000000000015``."""
        provider = self._provider(1_500_000)

        result = asyncio.run(provider.get_balance("arbitrum", "USDT", WALLET))

        assert result == Decimal("1.5")

    def test_usdc_is_unchanged(self):
        provider = self._provider(1_500_000)

        assert asyncio.run(provider.get_balance("arbitrum", "USDC", WALLET)) == Decimal("1.5")

    def test_weth_is_unchanged(self):
        provider = self._provider(10**18)

        assert asyncio.run(provider.get_balance("arbitrum", "WETH", WALLET)) == Decimal("1")

    def test_unknown_token_raises(self):
        provider = self._provider(10**18)

        with pytest.raises(BridgeBalanceUnavailableError, match="Refusing to guess"):
            asyncio.run(provider.get_balance("arbitrum", "NOT_A_REAL_TOKEN", WALLET))

    def test_wallet_is_restored_after_a_decimals_failure(self):
        """The ``finally`` must still run when decimals resolution raises."""
        provider = self._provider(10**18)
        original = provider._wallet_address
        other = "0x" + "22" * 20

        with pytest.raises(BridgeBalanceUnavailableError):
            asyncio.run(provider.get_balance("arbitrum", "NOT_A_REAL_TOKEN", other))

        assert provider._wallet_address == original


class TestRegistryAssumptions:
    """Guards for the addresses the tests above lean on."""

    def test_arbitrum_entries_exist(self):
        assert {"USDC", "WETH", "ETH"} <= set(TOKEN_ADDRESSES["arbitrum"])
