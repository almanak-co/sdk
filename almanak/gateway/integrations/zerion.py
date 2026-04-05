"""Zerion wallet portfolio integration for the gateway.

Provides cached, rate-limited access to wallet portfolio totals and
protocol-aware positions. Response parsing is intentionally defensive:
Zerion's payloads are normalized into simple Almanak dataclasses so
framework and dashboard code do not depend on vendor-specific shapes.
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.gateway.integrations.base import BaseIntegration
from almanak.gateway.utils.rpc_provider import _get_gateway_api_key

logger = logging.getLogger(__name__)


@dataclass
class ZerionPosition:
    """Normalized wallet position from Zerion."""

    position_id: str
    protocol: str
    label: str
    position_type: str
    value_usd: str
    pool_address: str = ""
    token_symbols: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ZerionPortfolioSnapshot:
    """Normalized Zerion wallet portfolio payload."""

    provider: str
    wallet_address: str
    chain: str
    total_value_usd: str
    positions: list[ZerionPosition] = field(default_factory=list)
    cache_hit: bool = False
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class ZerionIntegration(BaseIntegration):
    """Gateway client for Zerion portfolio APIs."""

    name = "zerion"
    rate_limit_requests = 120
    default_cache_ttl = 300
    _API_BASE = "https://api.zerion.io"

    _CHAIN_IDS = {
        "ethereum": "ethereum",
        "arbitrum": "arbitrum",
        "optimism": "optimism",
        "base": "base",
        "avalanche": "avalanche",
        "polygon": "polygon",
        "bsc": "binance-smart-chain",
        "bnb": "binance-smart-chain",
        "solana": "solana",
        "sonic": "sonic",
        "plasma": "plasma",
    }

    def __init__(self, api_key: str | None = None, request_timeout: float = 30.0, cache_ttl: int | None = None) -> None:
        if api_key is None:
            api_key = _get_gateway_api_key("PORTFOLIO_API_KEY") or _get_gateway_api_key("ZERION_API_KEY")

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
            encoded = base64.b64encode(f"{self._api_key}:".encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"
        return headers

    @property
    def is_configured(self) -> bool:
        """Return True when an API key is set."""
        return bool(self._api_key)

    async def health_check(self) -> bool:
        """Return True when the integration has an API key configured.

        This is a configuration check only — it does not verify network
        reachability or API key validity.  Callers that need a liveness
        probe should make a real API request instead.
        """
        return bool(self._api_key)

    @staticmethod
    def _cache_address(wallet_address: str, chain: str) -> str:
        """Normalize wallet address for cache keys. Preserves case for Solana (base58)."""
        if chain.lower() == "solana":
            return wallet_address
        return wallet_address.lower()

    async def get_wallet_positions(self, wallet_address: str, chain: str) -> ZerionPortfolioSnapshot:
        """Get normalized positions for a wallet on a chain."""
        cache_addr = self._cache_address(wallet_address, chain)
        cache_key = f"positions:{cache_addr}:{chain.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            cached.cache_hit = True
            return cached

        chain_id = self._CHAIN_IDS.get(chain.lower(), chain.lower())
        data = await self._fetch(
            f"/v1/wallets/{wallet_address}/positions",
            params={"filter[chain_ids]": chain_id, "filter[positions]": "no_filter"},
        )
        snapshot = self._normalize_positions(wallet_address, chain, data)
        self._update_cache(cache_key, snapshot)
        return snapshot

    async def get_wallet_portfolio(self, wallet_address: str, chain: str) -> ZerionPortfolioSnapshot:
        """Get wallet portfolio total and, when available, embedded positions."""
        cache_addr = self._cache_address(wallet_address, chain)
        cache_key = f"portfolio:{cache_addr}:{chain.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            cached.cache_hit = True
            return cached

        chain_id = self._CHAIN_IDS.get(chain.lower(), chain.lower())
        data = await self._fetch(
            f"/v1/wallets/{wallet_address}/portfolio",
            params={"filter[chain_ids]": chain_id},
        )
        snapshot = self._normalize_portfolio(wallet_address, chain, data)
        self._update_cache(cache_key, snapshot)
        return snapshot

    def _normalize_positions(self, wallet_address: str, chain: str, payload: Any) -> ZerionPortfolioSnapshot:
        positions = [self._normalize_position(item) for item in self._extract_items(payload)]
        total = sum((self._to_decimal(p.value_usd) for p in positions), Decimal("0"))
        return ZerionPortfolioSnapshot(
            provider=self.name,
            wallet_address=wallet_address,
            chain=chain,
            total_value_usd=str(total),
            positions=positions,
            cache_hit=False,
        )

    def _normalize_portfolio(self, wallet_address: str, chain: str, payload: Any) -> ZerionPortfolioSnapshot:
        items = self._extract_items(payload)
        positions = [self._normalize_position(item) for item in items]
        total_value = self._extract_total_value(payload)
        if total_value == "0" and positions:
            total_value = str(sum((self._to_decimal(p.value_usd) for p in positions), Decimal("0")))

        return ZerionPortfolioSnapshot(
            provider=self.name,
            wallet_address=wallet_address,
            chain=chain,
            total_value_usd=total_value,
            positions=positions,
            cache_hit=False,
        )

    def _extract_items(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
            if isinstance(data, dict):
                embedded = data.get("positions") or data.get("items")
                if isinstance(embedded, list):
                    return [item for item in embedded if isinstance(item, dict)]
        return []

    def _extract_total_value(self, payload: Any) -> str:
        candidates: list[Any] = []
        if isinstance(payload, dict):
            candidates.extend(
                [
                    payload.get("total_value"),
                    payload.get("total_value_usd"),
                    payload.get("value"),
                ]
            )
            data = payload.get("data")
            if isinstance(data, dict):
                attributes = data.get("attributes", {})
                candidates.extend(
                    [
                        data.get("total_value"),
                        data.get("total_value_usd"),
                        data.get("value"),
                        attributes.get("total_value"),
                        attributes.get("total_value_usd"),
                        attributes.get("value"),
                    ]
                )

        for candidate in candidates:
            normalized = self._normalize_value(candidate)
            if normalized is not None:
                return normalized
        return "0"

    def _normalize_position(self, item: dict[str, Any]) -> ZerionPosition:
        attributes = item.get("attributes", {}) if isinstance(item.get("attributes"), dict) else {}
        protocol = self._extract_protocol(item, attributes)
        label = self._extract_label(item, attributes, protocol)
        position_type = str(
            attributes.get("position_type") or attributes.get("type") or item.get("type") or item.get("id") or "UNKNOWN"
        )
        value_usd = self._extract_value(item, attributes)
        pool_address = self._extract_pool_address(item, attributes)
        token_symbols = self._extract_token_symbols(item, attributes)
        position_id = str(item.get("id") or attributes.get("id") or f"{protocol}:{label}")
        details = self._curate_details(item, attributes)

        return ZerionPosition(
            position_id=position_id,
            protocol=protocol,
            label=label,
            position_type=position_type,
            value_usd=value_usd,
            pool_address=pool_address,
            token_symbols=token_symbols,
            details=details,
        )

    def _extract_protocol(self, item: dict[str, Any], attributes: dict[str, Any]) -> str:
        for candidate in (
            item.get("protocol"),
            attributes.get("protocol"),
            attributes.get("protocol_slug"),
            attributes.get("protocol_name"),
        ):
            if isinstance(candidate, dict):
                candidate = candidate.get("name") or candidate.get("slug") or candidate.get("id")
            if candidate:
                return str(candidate)
        relationships = item.get("relationships")
        if isinstance(relationships, dict):
            protocol = relationships.get("protocol")
            if isinstance(protocol, dict):
                data = protocol.get("data", {})
                if isinstance(data, dict):
                    candidate = data.get("id") or data.get("name")
                    if candidate:
                        return str(candidate)
        return "unknown"

    def _extract_label(self, item: dict[str, Any], attributes: dict[str, Any], protocol: str) -> str:
        for candidate in (
            attributes.get("name"),
            attributes.get("label"),
            attributes.get("display_name"),
            item.get("name"),
            item.get("id"),
        ):
            if candidate:
                return str(candidate)
        return protocol

    def _extract_value(self, item: dict[str, Any], attributes: dict[str, Any]) -> str:
        candidates = [
            attributes.get("value"),
            attributes.get("value_usd"),
            attributes.get("usd_value"),
            item.get("value"),
            item.get("value_usd"),
        ]
        nested_values = [
            attributes.get("stats"),
            attributes.get("totals"),
            attributes.get("balance"),
        ]
        for nested in nested_values:
            if isinstance(nested, dict):
                candidates.extend(
                    [
                        nested.get("value"),
                        nested.get("value_usd"),
                        nested.get("usd_value"),
                        nested.get("total"),
                    ]
                )
        for candidate in candidates:
            normalized = self._normalize_value(candidate)
            if normalized is not None:
                return normalized
        return "0"

    def _extract_pool_address(self, item: dict[str, Any], attributes: dict[str, Any]) -> str:
        candidates = [
            attributes.get("pool_address"),
            attributes.get("market_address"),
            attributes.get("vault_address"),
            attributes.get("address"),
        ]
        relationships = item.get("relationships")
        if isinstance(relationships, dict):
            for key in ("pool", "market", "vault"):
                rel = relationships.get(key)
                if isinstance(rel, dict):
                    data = rel.get("data")
                    if isinstance(data, dict):
                        candidates.extend([data.get("id"), data.get("address")])
        for candidate in candidates:
            if candidate:
                return str(candidate)
        return ""

    @staticmethod
    def _curate_details(item: dict[str, Any], attributes: dict[str, Any]) -> dict[str, Any]:
        """Extract a curated subset of details instead of storing the full raw payload."""
        curated: dict[str, Any] = {}
        for key in ("position_type", "type", "protocol_slug", "protocol_name", "pool_address", "market_address"):
            if key in attributes and attributes[key] is not None:
                curated[key] = attributes[key] if not isinstance(attributes[key], dict) else str(attributes[key])
        fungible_info = attributes.get("fungible_info")
        if isinstance(fungible_info, dict):
            curated["fungible_name"] = fungible_info.get("name")
            curated["fungible_symbol"] = fungible_info.get("symbol")
        if item.get("id"):
            curated["zerion_id"] = item["id"]
        return curated

    _MAX_SYMBOL_DEPTH = 10

    def _extract_token_symbols(self, item: dict[str, Any], attributes: dict[str, Any]) -> list[str]:
        symbols: list[str] = []

        def _visit(value: Any, depth: int = 0) -> None:
            if depth > self._MAX_SYMBOL_DEPTH:
                return
            if isinstance(value, dict):
                for key, nested in value.items():
                    if key == "symbol" and nested:
                        symbols.append(str(nested))
                    else:
                        _visit(nested, depth + 1)
            elif isinstance(value, list):
                for nested in value:
                    _visit(nested, depth + 1)

        _visit(attributes)
        relationships = item.get("relationships")
        if isinstance(relationships, dict):
            _visit(relationships)

        deduped: list[str] = []
        seen: set[str] = set()
        for symbol in symbols:
            if symbol not in seen:
                deduped.append(symbol)
                seen.add(symbol)
        return deduped

    def _normalize_value(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, dict):
            for key in ("usd", "value", "amount"):
                if key in value:
                    return self._normalize_value(value.get(key))
            return None
        try:
            return str(self._to_decimal(value))
        except InvalidOperation:
            return None

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        return Decimal(str(value))

    @staticmethod
    def to_dict(snapshot: ZerionPortfolioSnapshot) -> dict[str, Any]:
        """Convert a snapshot to a JSON-serializable dict for debugging/tests."""
        return {
            "provider": snapshot.provider,
            "wallet_address": snapshot.wallet_address,
            "chain": snapshot.chain,
            "total_value_usd": snapshot.total_value_usd,
            "cache_hit": snapshot.cache_hit,
            "fetched_at": snapshot.fetched_at.isoformat(),
            "positions": [
                {
                    **{k: v for k, v in asdict(position).items() if k != "details"},
                    "raw_details_json": json.dumps(position.details, sort_keys=True, default=str),
                }
                for position in snapshot.positions
            ],
        }
