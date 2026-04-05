"""Moralis wallet portfolio integration for the gateway.

Provides cached, rate-limited access to wallet token balances via the
Moralis Web3 API. Unlike Zerion (single-call portfolio), Moralis uses
per-chain token balance endpoints, so each chain is a separate API call.

Free tier: 40K CU/day (~13K simple calls at 3 CU each).
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.gateway.integrations.base import BaseIntegration
from almanak.gateway.integrations.models import WalletPortfolioSnapshot, WalletPosition
from almanak.gateway.utils.rpc_provider import _get_gateway_api_key

logger = logging.getLogger(__name__)


class MoralisIntegration(BaseIntegration):
    """Gateway client for Moralis Web3 Data API (wallet token balances)."""

    name = "moralis"
    rate_limit_requests = 120
    default_cache_ttl = 60
    _API_BASE = "https://deep-index.moralis.io/api/v2.2"

    # Moralis chain identifiers
    # https://docs.moralis.io/supported-chains
    _CHAIN_IDS: dict[str, str] = {
        "ethereum": "0x1",
        "polygon": "0x89",
        "bsc": "0x38",
        "bnb": "0x38",
        "avalanche": "0xa86a",
        "arbitrum": "0xa4b1",
        "optimism": "0xa",
        "base": "0x2105",
        "sonic": "0x92",
        "solana": "solana",
    }

    def __init__(
        self,
        api_key: str | None = None,
        request_timeout: float = 30.0,
        cache_ttl: int | None = None,
    ) -> None:
        if api_key is None:
            api_key = _get_gateway_api_key("MORALIS_API_KEY")

        super().__init__(
            api_key=api_key,
            base_url=self._API_BASE,
            request_timeout=request_timeout,
        )
        if cache_ttl is not None:
            self.default_cache_ttl = cache_ttl

    def _get_headers(self) -> dict[str, str]:
        headers = super()._get_headers()
        if self._api_key:
            headers["X-API-Key"] = self._api_key
        return headers

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)

    def supports_portfolio(self) -> bool:
        return True

    async def health_check(self) -> bool:
        return bool(self._api_key)

    async def get_wallet_portfolio(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Get wallet token balances as a portfolio snapshot."""
        cache_key = f"moralis:portfolio:{wallet_address.lower()}:{chain.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            cached.cache_hit = True
            return cached

        chain_id = self._CHAIN_IDS.get(chain.lower())
        if not chain_id:
            logger.warning("Moralis: unsupported chain %s, using as-is", chain)
            chain_id = chain.lower()

        if chain_id == "solana":
            snapshot = await self._fetch_solana_portfolio(wallet_address, chain)
        else:
            snapshot = await self._fetch_evm_portfolio(wallet_address, chain, chain_id)

        self._update_cache(cache_key, snapshot)
        return snapshot

    async def get_wallet_positions(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Positions endpoint — Moralis only has token balances, so this delegates to portfolio."""
        return await self.get_wallet_portfolio(wallet_address, chain)

    async def _fetch_evm_portfolio(self, wallet_address: str, chain: str, chain_id: str) -> WalletPortfolioSnapshot:
        """Fetch EVM token balances with USD prices."""
        data = await self._fetch(
            f"/{wallet_address}/erc20",
            params={"chain": chain_id},
        )
        return self._normalize_evm_response(wallet_address, chain, data)

    async def _fetch_solana_portfolio(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Fetch Solana token balances."""
        data = await self._fetch(
            f"/account/mainnet/{wallet_address}/tokens",
        )
        return self._normalize_solana_response(wallet_address, chain, data)

    def _normalize_evm_response(self, wallet_address: str, chain: str, data: Any) -> WalletPortfolioSnapshot:
        """Normalize Moralis EVM token balance response."""
        items = data if isinstance(data, list) else []
        positions: list[WalletPosition] = []
        total = Decimal("0")

        for item in items:
            if not isinstance(item, dict):
                continue

            symbol = item.get("symbol", "UNKNOWN")
            name = str(item.get("name") or symbol)
            token_address = item.get("token_address", "")
            raw_decimals = item.get("decimals")
            if raw_decimals is None:
                logger.warning("Moralis: missing decimals for %s (%s), skipping valuation", symbol, token_address)
                decimals = 0
            else:
                decimals = int(raw_decimals)

            # Calculate USD value from balance and usd_price
            raw_balance = item.get("balance", "0")
            usd_price = item.get("usd_price")
            if raw_decimals is None:
                value_usd = "0"
            else:
                value_usd = self._calc_usd_value(raw_balance, decimals, usd_price)

            total += self._safe_decimal(value_usd)

            positions.append(
                WalletPosition(
                    position_id=f"moralis:{token_address}",
                    protocol="wallet",
                    label=name,
                    position_type="token",
                    value_usd=value_usd,
                    pool_address=token_address,
                    token_symbols=[symbol] if symbol else [],
                    details={
                        "decimals": decimals,
                        "balance_raw": raw_balance,
                        "usd_price": str(usd_price) if usd_price is not None else None,
                    },
                )
            )

        return WalletPortfolioSnapshot(
            provider=self.name,
            wallet_address=wallet_address,
            chain=chain,
            total_value_usd=str(total),
            positions=positions,
            cache_hit=False,
        )

    def _normalize_solana_response(self, wallet_address: str, chain: str, data: Any) -> WalletPortfolioSnapshot:
        """Normalize Moralis Solana token response."""
        items = data if isinstance(data, list) else []
        positions: list[WalletPosition] = []
        total = Decimal("0")

        for item in items:
            if not isinstance(item, dict):
                continue

            symbol = item.get("symbol", "UNKNOWN")
            name = str(item.get("name") or symbol)
            mint = item.get("mint", "")
            raw_decimals = item.get("decimals")
            if raw_decimals is None:
                logger.warning("Moralis: missing decimals for %s (%s), skipping valuation", symbol, mint)
                decimals = 0
            else:
                decimals = int(raw_decimals)

            raw_amount = item.get("amount", "0")
            usd_price = item.get("usd_price")
            if raw_decimals is None:
                value_usd = "0"
            else:
                value_usd = self._calc_usd_value(raw_amount, decimals, usd_price)

            total += self._safe_decimal(value_usd)

            positions.append(
                WalletPosition(
                    position_id=f"moralis:{mint}",
                    protocol="wallet",
                    label=name,
                    position_type="token",
                    value_usd=value_usd,
                    pool_address=mint,
                    token_symbols=[symbol] if symbol else [],
                    details={
                        "decimals": decimals,
                        "amount_raw": raw_amount,
                        "usd_price": str(usd_price) if usd_price is not None else None,
                    },
                )
            )

        return WalletPortfolioSnapshot(
            provider=self.name,
            wallet_address=wallet_address,
            chain=chain,
            total_value_usd=str(total),
            positions=positions,
            cache_hit=False,
        )

    @staticmethod
    def _calc_usd_value(raw_balance: str, decimals: int, usd_price: Any) -> str:
        """Calculate USD value from raw balance, decimals, and price."""
        try:
            balance = Decimal(str(raw_balance)) / Decimal(10) ** decimals
            if usd_price is not None:
                return str(balance * Decimal(str(usd_price)))
            return "0"
        except (InvalidOperation, ValueError, ZeroDivisionError):
            return "0"

    @staticmethod
    def _safe_decimal(value: str) -> Decimal:
        try:
            return Decimal(value)
        except (InvalidOperation, ValueError):
            return Decimal("0")
