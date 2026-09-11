"""Wallet request provenance for one portfolio snapshot capture."""

from decimal import Decimal
from typing import Any

from almanak.core.chains._helpers import is_solana_chain


class WalletScopeCapture:
    """Retain observed request identities without assigning aggregated rows a wallet."""

    def __init__(self) -> None:
        self._chain_wallets: dict[str, set[str]] = {}
        self._token_spellings: dict[str, set[str]] = {}
        self._token_scopes: dict[str, set[tuple[str, str] | None]] = {}

    def observe(self, token: str, requested_chain: str, result: Any) -> None:
        chain = getattr(result, "chain", None)
        wallet = getattr(result, "wallet_address", None)
        scope = None
        balance = getattr(result, "balance", None)
        measured = isinstance(balance, Decimal) and balance.is_finite() and balance >= 0
        if measured and isinstance(chain, str) and isinstance(wallet, str) and wallet.strip():
            chain = chain.strip().lower()
            wallet = wallet.strip()
            if chain and chain == requested_chain.strip().lower():
                # Solana addresses are case-sensitive and cannot inherit an EVM fallback.
                solana_family = is_solana_chain(chain)
                if not solana_family or not wallet.lower().startswith("0x"):
                    wallet = wallet if solana_family else wallet.lower()
                    self._chain_wallets.setdefault(chain, set()).add(wallet)
                    scope = (chain, wallet)
        self._token_spellings.setdefault(token.upper(), set()).add(token)
        self._token_scopes.setdefault(token.upper(), set()).add(scope)

    def apply(self, rows: list[Any]) -> None:
        for row in rows:
            scopes = self._token_scopes.get(row.symbol.upper(), set())
            if len(self._token_spellings.get(row.symbol.upper(), set())) != 1:
                continue
            if len(scopes) == 1 and None not in scopes:
                scope = next(iter(scopes))
                if scope is None:
                    continue
                chain, wallet = scope
                if self._chain_wallets.get(chain) == {wallet}:
                    row.chain, row.wallet_address = chain, wallet

    def metadata(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "chain_wallets": {
                chain: next(iter(wallets)) for chain, wallets in self._chain_wallets.items() if len(wallets) == 1
            },
        }
