"""Moralis wallet portfolio integration for the gateway.

Provides cached, rate-limited access to wallet token balances, DeFi positions,
and net-worth via the Moralis Web3 API v2.2.

EVM endpoints use the Wallet API (slug-based chain params):
- GET /wallets/{address}/tokens — token balances with USD prices
- GET /wallets/{address}/defi/positions — DeFi protocol positions
- GET /wallets/{address}/net-worth — total portfolio value

Solana uses the legacy account endpoint (unchanged).

Free tier: 40K CU/day. Token endpoint: 100 CU, DeFi: 50 CU, Net-worth: 250 CU/chain.
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import external_id_for
from almanak.core.enums import ChainFamily
from almanak.gateway.integrations.base import BaseIntegration, IntegrationError
from almanak.gateway.integrations.models import WalletPortfolioSnapshot, WalletPosition
from almanak.gateway.utils.rpc_provider import _get_gateway_api_key

logger = logging.getLogger(__name__)


class MoralisIntegration(BaseIntegration):
    """Gateway client for Moralis Web3 Data API (wallet tokens, DeFi, net-worth)."""

    name = "moralis"
    rate_limit_requests = 120
    default_cache_ttl = 60
    _API_BASE = "https://deep-index.moralis.io/api/v2.2"

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

    def _is_solana(self, chain: str) -> bool:
        """Check if the chain is Solana."""
        descriptor = ChainRegistry.try_resolve(chain)
        return descriptor is not None and descriptor.family is ChainFamily.SOLANA

    def _get_chain_slug(self, chain: str) -> str | None:
        """Get the Moralis slug for an EVM chain. Returns None if unsupported.

        Moralis v2.2 Wallet API uses slug-based chain identifiers for EVM
        (https://docs.moralis.io/supported-chains). Resolved from the registry
        via ``ChainDescriptor.external_ids["moralis"]`` (VIB-4851 B1); alias-
        normalised so ``bnb`` -> ``bsc``, and Solana (no Moralis slug) -> None.
        """
        return external_id_for(chain, "moralis")

    # -------------------------------------------------------------------------
    # Portfolio API — net-worth endpoint
    # -------------------------------------------------------------------------

    async def get_wallet_portfolio(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Get wallet total portfolio value.

        For EVM: uses GET /wallets/{address}/net-worth (250 CU/chain).
        For Solana: falls back to token balance sum.
        """
        cache_key = f"moralis:portfolio:{wallet_address.lower()}:{chain.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            cached.cache_hit = True
            return cached

        if self._is_solana(chain):
            snapshot = await self._fetch_solana_portfolio(wallet_address, chain)
        else:
            snapshot = await self._fetch_evm_portfolio(wallet_address, chain)

        self._update_cache(cache_key, snapshot)
        return snapshot

    async def _fetch_evm_portfolio(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Fetch EVM portfolio via net-worth endpoint, with token-sum fallback."""
        chain_slug = self._get_chain_slug(chain)
        if not chain_slug:
            logger.warning("Moralis: unsupported chain %s for net-worth, falling back to token sum", chain)
            return await self._fetch_evm_token_sum(wallet_address, chain)

        try:
            data = await self._fetch(
                f"/wallets/{wallet_address}/net-worth",
                params={"chains": chain_slug, "exclude_spam": "true"},
            )
            return self._normalize_net_worth_response(wallet_address, chain, data)
        except Exception as e:
            logger.warning(
                "Moralis net-worth failed for %s on %s: %s, falling back to token sum", wallet_address, chain, e
            )
            return await self._fetch_evm_token_sum(wallet_address, chain)

    async def _fetch_evm_token_sum(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Fallback: sum token balances from the tokens endpoint."""
        snapshot = await self._fetch_evm_tokens(wallet_address, chain)
        return snapshot

    # -------------------------------------------------------------------------
    # Positions API — token balances + DeFi positions
    # -------------------------------------------------------------------------

    async def get_wallet_positions(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Get token balances + DeFi positions for a wallet on a chain.

        Combines two API calls:
        1. GET /wallets/{address}/tokens — bare token holdings (100 CU)
        2. GET /wallets/{address}/defi/positions — DeFi protocol positions (50 CU, best-effort)
        """
        cache_key = f"moralis:positions:{wallet_address.lower()}:{chain.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            cached.cache_hit = True
            return cached

        if self._is_solana(chain):
            snapshot = await self._fetch_solana_portfolio(wallet_address, chain)
            self._update_cache(cache_key, snapshot)
            return snapshot

        # Fetch token balances
        token_snapshot = await self._fetch_evm_tokens(wallet_address, chain)

        # Fetch DeFi positions (best-effort — don't fail the whole call)
        defi_positions: list[WalletPosition] = []
        try:
            defi_positions = await self._fetch_defi_positions(wallet_address, chain)
        except Exception as e:
            logger.warning("Moralis DeFi positions fetch failed for %s on %s: %s", wallet_address, chain, e)

        # Merge
        all_positions = token_snapshot.positions + defi_positions
        total = sum((self._safe_decimal(p.value_usd) for p in all_positions), Decimal("0"))

        snapshot = WalletPortfolioSnapshot(
            provider=self.name,
            wallet_address=wallet_address,
            chain=chain,
            total_value_usd=str(total),
            positions=all_positions,
            cache_hit=False,
        )
        self._update_cache(cache_key, snapshot)
        return snapshot

    # -------------------------------------------------------------------------
    # DeFi positions endpoint
    # -------------------------------------------------------------------------

    async def get_defi_positions(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Get DeFi protocol positions for a wallet on a chain."""
        cache_key = f"moralis:defi:{wallet_address.lower()}:{chain.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            cached.cache_hit = True
            return cached

        positions = await self._fetch_defi_positions(wallet_address, chain)
        total = sum((self._safe_decimal(p.value_usd) for p in positions), Decimal("0"))

        snapshot = WalletPortfolioSnapshot(
            provider=self.name,
            wallet_address=wallet_address,
            chain=chain,
            total_value_usd=str(total),
            positions=positions,
            cache_hit=False,
        )
        self._update_cache(cache_key, snapshot)
        return snapshot

    async def _fetch_defi_positions(self, wallet_address: str, chain: str) -> list[WalletPosition]:
        """Fetch DeFi positions via GET /wallets/{address}/defi/positions.

        Best-effort: returns empty list on 401/403 (may require Pro plan).
        """
        chain_slug = self._get_chain_slug(chain)
        if not chain_slug:
            logger.debug("Moralis: unsupported chain %s for DeFi positions", chain)
            return []

        try:
            data = await self._fetch(
                f"/wallets/{wallet_address}/defi/positions",
                params={"chain": chain_slug},
            )
            return self._normalize_defi_response(data)
        except IntegrationError as e:
            if e.code in ("HTTP_401", "HTTP_403"):
                logger.debug("Moralis DeFi positions not available (may require Pro plan): %s", e)
            else:
                logger.warning("Moralis DeFi positions error: %s", e)
            return []
        except Exception as e:
            logger.warning("Moralis DeFi positions error: %s", e)
            return []

    # -------------------------------------------------------------------------
    # Token balance fetching
    # -------------------------------------------------------------------------

    async def _fetch_evm_tokens(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Fetch EVM token balances via GET /wallets/{address}/tokens.

        Uses the v2.2 Wallet API which returns:
        - usd_price, usd_value, balance_formatted for each token
        - native_token flag for native chain tokens
        - possible_spam flag for spam tokens
        """
        chain_slug = self._get_chain_slug(chain)
        if not chain_slug:
            logger.warning("Moralis: unsupported chain %s, returning empty", chain)
            return WalletPortfolioSnapshot(
                provider=self.name,
                wallet_address=wallet_address,
                chain=chain,
                total_value_usd="0",
                positions=[],
                cache_hit=False,
            )

        data = await self._fetch(
            f"/wallets/{wallet_address}/tokens",
            params={"chain": chain_slug},
        )
        return self._normalize_evm_response(wallet_address, chain, data)

    async def _fetch_solana_portfolio(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Fetch Solana token balances (legacy endpoint, unchanged)."""
        data = await self._fetch(
            f"/account/mainnet/{wallet_address}/tokens",
        )
        return self._normalize_solana_response(wallet_address, chain, data)

    # -------------------------------------------------------------------------
    # Response normalization
    # -------------------------------------------------------------------------

    def _normalize_net_worth_response(self, wallet_address: str, chain: str, data: Any) -> WalletPortfolioSnapshot:
        """Normalize Moralis net-worth response.

        Response shape: {"total_networth_usd": "...", "chains": [{"chain": "...", "networth_usd": "..."}]}
        """
        total_value = "0"
        if isinstance(data, dict):
            total_value = str(data.get("total_networth_usd", "0"))

        return WalletPortfolioSnapshot(
            provider=self.name,
            wallet_address=wallet_address,
            chain=chain,
            total_value_usd=total_value,
            positions=[],
            cache_hit=False,
        )

    def _normalize_evm_response(self, wallet_address: str, chain: str, data: Any) -> WalletPortfolioSnapshot:
        """Normalize Moralis v2.2 Wallet API token response.

        New endpoint returns:
        - result[]: array of token objects
        - Each token has: usd_price, usd_value, balance_formatted, native_token, possible_spam
        - usd_value is pre-calculated — no manual calculation needed

        Also handles legacy flat-list format for backward compatibility.
        """
        # The new endpoint wraps results in {"result": [...]}
        if isinstance(data, dict):
            items = data.get("result", [])
        elif isinstance(data, list):
            # Legacy format: flat list
            items = data
        else:
            items = []

        positions: list[WalletPosition] = []
        total = Decimal("0")

        for item in items:
            if not isinstance(item, dict):
                continue

            # Skip spam tokens
            if item.get("possible_spam", False):
                continue

            symbol = item.get("symbol", "UNKNOWN")
            name = str(item.get("name") or symbol)
            token_address = item.get("token_address", "")
            is_native = item.get("native_token", False)

            # New endpoint provides usd_value directly (pre-calculated)
            usd_value_raw = item.get("usd_value")
            if usd_value_raw is not None:
                value_usd = str(usd_value_raw)
            else:
                # Fallback: calculate from balance + price (legacy or missing usd_value)
                raw_decimals = item.get("decimals")
                if raw_decimals is None:
                    logger.warning("Moralis: missing decimals for %s (%s), skipping valuation", symbol, token_address)
                    value_usd = "0"
                else:
                    decimals = int(raw_decimals)
                    raw_balance = item.get("balance", "0")
                    usd_price = item.get("usd_price")
                    value_usd = self._calc_usd_value(raw_balance, decimals, usd_price)

            total += self._safe_decimal(value_usd)

            # Build position details
            details: dict[str, Any] = {}
            if item.get("decimals") is not None:
                details["decimals"] = int(item["decimals"])
            if item.get("balance_formatted") is not None:
                details["balance_formatted"] = str(item["balance_formatted"])
            elif item.get("balance") is not None:
                details["balance_raw"] = str(item["balance"])
            if item.get("usd_price") is not None:
                details["usd_price"] = str(item["usd_price"])
            if is_native:
                details["native_token"] = True

            position_id_part = token_address if token_address else ("native" if is_native else "unknown")
            positions.append(
                WalletPosition(
                    position_id=f"moralis:{position_id_part}",
                    protocol="wallet",
                    label=name,
                    position_type="token",
                    value_usd=value_usd,
                    pool_address=token_address,
                    token_symbols=[symbol] if symbol else [],
                    details=details,
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

    def _normalize_defi_response(self, data: Any) -> list[WalletPosition]:
        """Normalize Moralis DeFi positions response.

        Response shape: [{"protocol_name": "...", "protocol_id": "...",
                          "position": {"label": "...", "tokens": [...],
                                       "balance_usd": ..., "total_unclaimed_usd_value": ...}}]
        """
        items = data if isinstance(data, list) else []
        positions: list[WalletPosition] = []

        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue

            protocol_name = item.get("protocol_name", "unknown")
            protocol_id = item.get("protocol_id", "")
            position = item.get("position", {})
            if not isinstance(position, dict):
                continue

            label = position.get("label", f"{protocol_name} position")
            balance_usd = str(position.get("balance_usd", "0"))
            unclaimed_usd = str(position.get("total_unclaimed_usd_value", "0"))

            # Extract token symbols
            symbols: list[str] = []
            tokens = position.get("tokens", [])
            if isinstance(tokens, list):
                for tok in tokens:
                    if isinstance(tok, dict) and tok.get("symbol"):
                        symbols.append(tok["symbol"])

            # Total value = balance + unclaimed
            total_value = self._safe_decimal(balance_usd) + self._safe_decimal(unclaimed_usd)

            # Extract pool/contract address from position_details if available
            position_details = item.get("position_details", {})
            pool_address: str = ""
            if isinstance(position_details, dict):
                pool_address = position_details.get("pool_address") or position_details.get("contract_address") or ""

            positions.append(
                WalletPosition(
                    position_id=f"moralis:defi:{protocol_id}:{label}:{idx}",
                    protocol=protocol_name,
                    label=label,
                    position_type="defi",
                    value_usd=str(total_value),
                    pool_address=pool_address,
                    token_symbols=symbols,
                    details={
                        "protocol_id": protocol_id,
                        "balance_usd": balance_usd,
                        "unclaimed_usd": unclaimed_usd,
                    },
                )
            )

        return positions

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
