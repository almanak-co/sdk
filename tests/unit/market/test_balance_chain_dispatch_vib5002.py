"""VIB-5002: ``MarketSnapshot.balance`` must thread ``chain`` into chain-aware
``get_balance`` providers.

``MultiChainGatewayBalanceProvider.get_balance(token, chain)`` requires the
chain. Before the fix, the dispatch always called ``bp.get_balance(token)`` with
a single arg whenever the provider exposed a ``get_balance`` method, raising
``TypeError: ... missing 1 required positional argument: 'chain'`` on every
multi-chain balance read (surfacing as ``balance_failed`` →
``ACCOUNTING_FAILED``). The dispatch must thread ``chain=`` when the
``get_balance`` signature accepts it, while leaving chain-agnostic providers on
the legacy single-arg path.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock

from almanak.framework.market import MarketSnapshot, TokenBalance
from almanak.framework.market.snapshot import _balance_provider_supports_chain_arg

WALLET = "0xdeadbeef1234567890abcdef1234567890abcdef"


class _ChainAwareProvider:
    """Mirrors ``MultiChainGatewayBalanceProvider``: ``get_balance(token, chain)``
    with ``chain`` REQUIRED (sync, returns ``TokenBalance``)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def get_balance(self, token: str, chain: str) -> TokenBalance:
        self.calls.append((token, chain))
        return TokenBalance(symbol=token, balance=Decimal("5"), balance_usd=Decimal("5"))

    def __call__(self, token: str, chain: str) -> TokenBalance:
        return self.get_balance(token, chain)


class _SingleChainProvider:
    """Chain-agnostic provider: ``get_balance(token)`` with NO chain param.

    Passing ``chain=`` would raise ``TypeError``, so this guards the fix against
    over-eagerly threading the chain into chain-agnostic providers.
    """

    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_balance(self, token: str) -> TokenBalance:
        self.calls.append(token)
        return TokenBalance(symbol=token, balance=Decimal("7"), balance_usd=Decimal("7"))


def test_chain_aware_get_balance_receives_chain() -> None:
    """The multi-chain provider's ``get_balance`` is called WITH the resolved chain."""
    provider = _ChainAwareProvider()
    snapshot = MarketSnapshot(
        wallet_address=WALLET,
        balance_provider=provider,
        chains=("base", "arbitrum"),
    )

    result = snapshot.balance("USDC", chain="base")

    assert result.balance == Decimal("5")
    assert provider.calls, "provider.get_balance was never called"
    _token, called_chain = provider.calls[-1]
    assert called_chain == "base", f"chain must be threaded through, got {called_chain!r}"


def test_single_arg_get_balance_not_given_chain() -> None:
    """A chain-agnostic ``get_balance(token)`` must NOT receive a ``chain=`` kwarg."""
    provider = _SingleChainProvider()
    snapshot = MarketSnapshot(
        chain="base",
        wallet_address=WALLET,
        balance_provider=provider,
    )

    # Must not raise TypeError from an unexpected ``chain=`` kwarg.
    result = snapshot.balance("USDC")

    assert result.balance == Decimal("7")
    assert len(provider.calls) == 1


def test_helper_true_for_chain_required_method() -> None:
    assert _balance_provider_supports_chain_arg(_ChainAwareProvider().get_balance) is True


def test_helper_false_for_chain_agnostic_method() -> None:
    assert _balance_provider_supports_chain_arg(_SingleChainProvider().get_balance) is False


def test_helper_true_for_varkw_callable() -> None:
    """``AsyncMock``'s signature is ``(*args, **kwargs)`` — VAR_KEYWORD accepts
    ``chain=``, which the mock harmlessly ignores (legacy data-layer fixtures)."""
    assert _balance_provider_supports_chain_arg(AsyncMock()) is True
