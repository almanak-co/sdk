"""Regression test for the Anvil balance-cache toggle.

Reviewer-flagged in PR #2351: ``MarketServiceServicer._get_balance_provider``
must instantiate :class:`Web3BalanceProvider` with ``cache_ttl=0`` when
``settings.network == "anvil"`` and let the default apply otherwise.

The five-second default cache TTL is fine in production (block cadence is
much slower than the window), but in anvil mode strategy tests submit
pre-read + tx + post-read inside the window, causing reconciliation to see
stale pre-tx balances. The conditional fix is the reason this test exists.
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.market_service import MarketServiceServicer


def _settings(network: str) -> GatewaySettings:
    return GatewaySettings(network=network, metrics_enabled=False, audit_enabled=False)


@pytest.mark.asyncio
async def test_anvil_mode_constructs_provider_with_cache_ttl_zero() -> None:
    """``network=='anvil'`` → ``Web3BalanceProvider(cache_ttl=0, ...)``."""
    svc = MarketServiceServicer(_settings("anvil"))

    with patch(
        "almanak.gateway.data.balance.Web3BalanceProvider", new_callable=MagicMock
    ) as mock_provider, patch("almanak.gateway.utils.get_rpc_url", return_value="http://localhost:8545"):
        await svc._get_balance_provider("base", "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266")

    mock_provider.assert_called_once()
    kwargs = mock_provider.call_args.kwargs
    assert kwargs["cache_ttl"] == 0, f"expected cache_ttl=0 on anvil, got {kwargs.get('cache_ttl')}"


@pytest.mark.asyncio
async def test_mainnet_mode_omits_cache_ttl_to_use_provider_default() -> None:
    """``network=='mainnet'`` → no ``cache_ttl`` kwarg, provider default applies."""
    svc = MarketServiceServicer(_settings("mainnet"))

    with patch(
        "almanak.gateway.data.balance.Web3BalanceProvider", new_callable=MagicMock
    ) as mock_provider, patch("almanak.gateway.utils.get_rpc_url", return_value="https://example.invalid"):
        await svc._get_balance_provider("base", "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266")

    mock_provider.assert_called_once()
    kwargs = mock_provider.call_args.kwargs
    assert "cache_ttl" not in kwargs, (
        f"mainnet must let Web3BalanceProvider's own default apply, got cache_ttl={kwargs.get('cache_ttl')}"
    )
