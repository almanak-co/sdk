"""OKX OnchainOS portfolio integration for the gateway.

Provides cached, rate-limited access to wallet token balances, DeFi positions,
and total portfolio value via two OKX OnchainOS API modules:

- **Market Balance API** (GET): Token balances per wallet/chain
- **Wallet DeFi API** (POST): DeFi protocol positions (LP, lending, staking, rewards)

Authentication uses HMAC-SHA256 signing with three credentials:
OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE.

API docs:
  - Balance: https://web3.okx.com/onchainos/dev-docs/market/balance-reference
  - DeFi:   https://web3.okx.com/onchainos/dev-docs/wallet/defi-user-asset-overview
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode

import aiohttp

from almanak.gateway.integrations.base import BaseIntegration, IntegrationError, IntegrationRateLimitError
from almanak.gateway.integrations.models import WalletPortfolioSnapshot, WalletPosition
from almanak.gateway.utils.rpc_provider import _get_gateway_api_key

logger = logging.getLogger(__name__)


_INVEST_TYPE_LABELS: dict[int, str] = {
    1: "save",
    2: "pool",
    3: "farm",
    4: "vault",
    5: "stake",
    6: "lending",
    7: "lock",
    8: "leveraged_farming",
}


@dataclass(frozen=True)
class OkxDefiContext:
    """Per-(platform, chain) context threaded through the DeFi normalization helpers.

    Built at levels 1-2 of the payload walk (wallet -> platform -> chain) and
    passed verbatim into the level-3 row extractors. Keeping the context
    immutable and explicit prevents accidental cross-level mutation and makes
    each extractor a pure function of (payload fragment, context).
    """

    platform_names: dict[str, str]
    platform_id: str
    chain_index: str


def _safe_decimal(value: Any) -> Decimal:
    """Module-local mirror of :meth:`OkxIntegration._safe_decimal`.

    Kept private to the module to avoid forcing the extractor helpers to
    reach back into ``OkxIntegration`` for a pure-value conversion.
    """
    if value is None:
        return Decimal("0")
    try:
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else Decimal("0")
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _sum_position_values(position_list: Any) -> str:
    """Module-local mirror of :meth:`OkxIntegration._sum_position_values`.

    Sums ``currencyAmount`` across every asset in every position; used as the
    totalValue fallback for investments missing or reporting a zero total.
    """
    if not isinstance(position_list, list):
        return "0"
    total = Decimal("0")
    for pos in position_list:
        if not isinstance(pos, dict):
            continue
        assets = pos.get("assetsTokenList")
        if isinstance(assets, list):
            for asset in assets:
                if isinstance(asset, dict):
                    total += _safe_decimal(asset.get("currencyAmount", "0"))
    return str(total)


def _extract_data_entries(payload: Any) -> list[dict[str, Any]]:
    """Normalize the outer ``data`` field of a DeFi-detail payload to a list.

    The real OKX API wraps a single entry in a dict; some responses use a
    list. Unknown shapes (missing/invalid) yield an empty list so the caller
    can treat every iteration uniformly.
    """
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def _resolve_protocol_and_symbols(invest: dict[str, Any], ctx: OkxDefiContext) -> tuple[str, list[str]]:
    """Resolve the protocol name and token symbols for one investment row.

    Walks ``investLogo`` per the OKX contract:

    - ``bottomRightLogoList[0].tokenName`` overrides the platform name for
      protocol attribution (e.g. "Aave" instead of "Aave V3" when OKX
      attributes the specific protocol inside the platform).
    - ``middleLogoList[*].tokenName`` supplies the token symbol list.
    - When ``middleLogoList`` is missing or empty, ``tokenList[*].tokenSymbol``
      is used as the fallback source for symbols.
    """
    protocol_name = ctx.platform_names.get(ctx.platform_id, "unknown")
    invest_logo = invest.get("investLogo")
    if isinstance(invest_logo, dict):
        bottom_right = invest_logo.get("bottomRightLogoList")
        if isinstance(bottom_right, list) and bottom_right:
            logo_name = bottom_right[0].get("tokenName")
            if logo_name:
                protocol_name = str(logo_name)

    symbols: list[str] = []
    if isinstance(invest_logo, dict):
        middle = invest_logo.get("middleLogoList")
        if isinstance(middle, list):
            for tok in middle:
                if isinstance(tok, dict) and tok.get("tokenName"):
                    symbols.append(tok["tokenName"])

    if not symbols:
        token_list = invest.get("tokenList")
        if isinstance(token_list, list):
            for tok in token_list:
                if isinstance(tok, dict) and tok.get("tokenSymbol"):
                    symbols.append(tok["tokenSymbol"])

    return protocol_name, symbols


def _extract_position_rows(invest: dict[str, Any], ctx: OkxDefiContext) -> list[WalletPosition]:
    """Build the primary investment row(s) for one ``investTokenBalanceVoList`` entry.

    Returns exactly one :class:`WalletPosition` for a valid investment. The
    return type is a list so the caller can ``extend`` without special-casing
    empty responses in the future.

    NOTE: Preserves issue #1707 (measured ``totalValue == "0"`` recomputes
    from positionList). Do not change behavior here — the characterization
    test pins it.
    """
    inv_name = invest.get("investmentName", "")
    inv_id = str(invest.get("investmentId", ""))
    inv_type_int = invest.get("investType", 0)
    inv_type = _INVEST_TYPE_LABELS.get(inv_type_int, f"type_{inv_type_int}")

    protocol_name, symbols = _resolve_protocol_and_symbols(invest, ctx)

    raw_value = invest.get("totalValue", "")
    total_value = str(_safe_decimal(raw_value)) if raw_value else ""
    if not total_value or total_value == "0":
        # Issue #1707: measured zero collapses to a recompute.
        total_value = _sum_position_values(invest.get("positionList"))

    pool_addr = invest.get("poolAddress", "") or invest.get("tokenAddress", "")

    return [
        WalletPosition(
            position_id=f"okx:defi:{ctx.platform_id}:{inv_id}",
            protocol=protocol_name,
            label=inv_name or f"{protocol_name} {inv_type}",
            position_type=inv_type,
            value_usd=total_value,
            pool_address=pool_addr,
            token_symbols=symbols,
            details={
                "invest_type": inv_type,
                "invest_type_id": inv_type_int,
                "investment_id": inv_id,
                "chain_index": ctx.chain_index,
                "platform_id": ctx.platform_id,
            },
        )
    ]


def _extract_reward_rows_from_position(invest: dict[str, Any], ctx: OkxDefiContext) -> list[WalletPosition]:
    """Emit reward rows from ``positionList[].unclaimFeesDefiTokenInfo[].baseDefiTokenInfos[]``.

    One row per non-zero reward. ``protocol`` mirrors the investment-level
    resolution (via :func:`_resolve_protocol_and_symbols`), so rewards inherit
    the protocol attribution rather than falling back to the platform name.
    """
    rows: list[WalletPosition] = []

    protocol_name, _symbols = _resolve_protocol_and_symbols(invest, ctx)
    inv_id = str(invest.get("investmentId", ""))

    pos_list = invest.get("positionList")
    if not isinstance(pos_list, list):
        return rows

    for pos in pos_list:
        if not isinstance(pos, dict):
            continue
        unclaim = pos.get("unclaimFeesDefiTokenInfo")
        if not isinstance(unclaim, list):
            continue
        for fee_group in unclaim:
            if not isinstance(fee_group, dict):
                continue
            base_infos = fee_group.get("baseDefiTokenInfos")
            if not isinstance(base_infos, list):
                continue
            for reward in base_infos:
                if not isinstance(reward, dict):
                    continue
                r_symbol = reward.get("tokenSymbol", "UNKNOWN")
                r_amount = reward.get("coinAmount", "0")
                r_value = reward.get("currencyAmount", "0")
                if _safe_decimal(r_value) <= 0:
                    continue
                rows.append(
                    WalletPosition(
                        position_id=f"okx:reward:{ctx.platform_id}:{inv_id}:{r_symbol}",
                        protocol=protocol_name,
                        label=f"{protocol_name} reward",
                        position_type="reward",
                        value_usd=r_value,
                        token_symbols=[r_symbol],
                        details={
                            "reward_amount": r_amount,
                            "chain_index": ctx.chain_index,
                        },
                    )
                )

    return rows


def _extract_reward_rows_from_network(rewards: Any, ctx: OkxDefiContext) -> list[WalletPosition]:
    """Emit reward rows from ``networkHoldVoList[].availableRewards[]``.

    Network-level rewards use ``ctx.platform_names[platform_id]`` for
    attribution rather than the per-investment ``investLogo`` override, so
    the label/protocol falls back to the platform name.

    NOTE: Issue #1708 — these may duplicate rewards already emitted at the
    position level. Do not filter here; the characterization tests pin the
    duplicate behavior.
    """
    rows: list[WalletPosition] = []
    if not isinstance(rewards, list):
        return rows

    platform_label = ctx.platform_names.get(ctx.platform_id, "unknown")

    for reward in rewards:
        if not isinstance(reward, dict):
            continue
        r_symbol = reward.get("tokenSymbol", "UNKNOWN")
        r_amount = reward.get("tokenAmount", reward.get("coinAmount", "0"))
        r_value = reward.get("currencyAmount", "0")
        if _safe_decimal(r_value) <= 0:
            continue
        rows.append(
            WalletPosition(
                position_id=f"okx:reward:{ctx.platform_id}:{r_symbol}",
                protocol=platform_label,
                label=f"{platform_label} reward",
                position_type="reward",
                value_usd=r_value,
                token_symbols=[r_symbol],
                details={
                    "reward_amount": r_amount,
                    "chain_index": ctx.chain_index,
                },
            )
        )

    return rows


class OkxIntegration(BaseIntegration):
    """Gateway client for OKX OnchainOS Balance + DeFi APIs."""

    name = "okx"
    rate_limit_requests = 60  # Conservative; trial tier is 1-5 RPS
    default_cache_ttl = 60
    _API_BASE = "https://web3.okx.com"

    # OKX uses standard EVM numeric chain IDs as strings.
    # Non-EVM chains use OKX-specific synthetic IDs (e.g. Solana = "501").
    # https://web3.okx.com/onchainos/dev-docs/home/supported-chain
    _CHAIN_IDS: dict[str, str] = {
        "ethereum": "1",
        "optimism": "10",
        "bsc": "56",
        "bnb": "56",
        "polygon": "137",
        "base": "8453",
        "arbitrum": "42161",
        "avalanche": "43114",
        "sonic": "146",
        "solana": "501",
    }

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        api_passphrase: str | None = None,
        request_timeout: float = 30.0,
        cache_ttl: int | None = None,
    ) -> None:
        if api_key is None:
            api_key = _get_gateway_api_key("OKX_API_KEY")
        if api_secret is None:
            api_secret = _get_gateway_api_key("OKX_API_SECRET")
        if api_passphrase is None:
            api_passphrase = _get_gateway_api_key("OKX_API_PASSPHRASE")

        super().__init__(
            api_key=api_key,
            base_url=self._API_BASE,
            request_timeout=request_timeout,
        )
        self._api_secret = api_secret or ""
        self._api_passphrase = api_passphrase or ""

        if cache_ttl is not None:
            self.default_cache_ttl = cache_ttl

    @property
    def is_configured(self) -> bool:
        """Return True when all three OKX credentials are set."""
        return bool(self._api_key and self._api_secret and self._api_passphrase)

    def supports_portfolio(self) -> bool:
        return True

    async def health_check(self) -> bool:
        return self.is_configured

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        """Compute HMAC-SHA256 signature for OKX OnchainOS API.

        Signature = Base64(HMAC-SHA256(secret, timestamp + method + requestPath + body))
        """
        prehash = timestamp + method.upper() + request_path + body
        mac = hmac.new(
            self._api_secret.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode("utf-8")

    def _get_headers(self) -> dict[str, str]:
        """Return base headers without auth (auth added per-request in _fetch)."""
        return {
            "Accept": "application/json",
            "User-Agent": "Almanak-Gateway/1.0",
        }

    def _get_auth_headers(self, method: str, request_path: str, body: str = "") -> dict[str, str]:
        """Build full headers including HMAC-SHA256 authentication."""
        now = datetime.now(UTC)
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
        signature = self._sign(timestamp, method, request_path, body)

        headers = self._get_headers()
        headers.update(
            {
                "OK-ACCESS-KEY": self._api_key or "",
                "OK-ACCESS-SIGN": signature,
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": self._api_passphrase,
            }
        )
        return headers

    async def _fetch(
        self,
        path: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
    ) -> Any:
        """Override BaseIntegration._fetch to inject per-request HMAC auth headers.

        Includes automatic retry on HTTP 429 (inherits retry config from BaseIntegration).
        """
        self._metrics.total_requests += 1
        url = f"{self._base_url}{path}"

        for attempt in range(1 + self.rate_limit_max_retries):
            start_time = time.time()

            wait_time = await self._rate_limiter.acquire()
            if wait_time > 0:
                logger.debug("Rate limiter wait for %s: %.2fs", self.name, wait_time)

            # Build full URL with query string for signature
            if params:
                query_string = urlencode(params)
                request_path = f"{path}?{query_string}"
                full_url = f"{self._base_url}{request_path}"
            else:
                request_path = path
                full_url = url

            body = ""
            if json_data:
                body = json.dumps(json_data)

            headers = self._get_auth_headers(method, request_path, body)
            if body:
                headers["Content-Type"] = "application/json"

            try:
                session = await self._get_session()
                async with session.request(
                    method,
                    full_url,
                    data=body if body else None,
                    headers=headers,
                ) as response:
                    latency_ms = (time.time() - start_time) * 1000

                    if response.status == 429:
                        try:
                            retry_after = float(response.headers.get("Retry-After", "5"))
                        except (ValueError, TypeError):
                            retry_after = 5.0
                        retry_after = min(max(retry_after, 0), self.rate_limit_max_wait)

                        if attempt < self.rate_limit_max_retries:
                            logger.info(
                                "%s rate limited on %s, retrying in %.1fs (attempt %d/%d)",
                                self.name,
                                path,
                                retry_after,
                                attempt + 1,
                                self.rate_limit_max_retries,
                            )
                            await asyncio.sleep(retry_after)
                            continue

                        self._metrics.rate_limited_requests += 1
                        self._metrics.failed_requests += 1
                        self._metrics.last_error = f"Rate limited after {self.rate_limit_max_retries} retries"
                        self._metrics.last_error_time = datetime.now(UTC)
                        raise IntegrationRateLimitError(self.name, retry_after)

                    if response.status >= 400:
                        error_text = await response.text()
                        self._metrics.failed_requests += 1
                        self._metrics.last_error = f"HTTP {response.status}: {error_text}"
                        self._metrics.last_error_time = datetime.now(UTC)
                        raise IntegrationError(
                            self.name,
                            f"HTTP {response.status}: {error_text}",
                            code=f"HTTP_{response.status}",
                        )

                    try:
                        data = await response.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError) as e:
                        self._metrics.failed_requests += 1
                        self._metrics.last_error = f"Invalid JSON response: {e}"
                        self._metrics.last_error_time = datetime.now(UTC)
                        raise IntegrationError(
                            self.name,
                            f"Invalid JSON response from OKX: {e}",
                            code="INVALID_RESPONSE",
                        ) from e

                    # OKX responses must be a dict with "code" and "data" keys.
                    # Reject unexpected envelopes (e.g. {}, []) to trigger provider failover.
                    if not isinstance(data, dict) or "code" not in data:
                        self._metrics.failed_requests += 1
                        self._metrics.last_error = f"Invalid OKX response envelope: {type(data).__name__}"
                        self._metrics.last_error_time = datetime.now(UTC)
                        raise IntegrationError(
                            self.name,
                            f"Invalid OKX response: expected dict with 'code', got {type(data).__name__}",
                            code="INVALID_RESPONSE",
                        )

                    # OKX returns HTTP 200 with error codes in the body
                    # (e.g., {"code": "50011", "msg": "Invalid API key"})
                    okx_code = str(data["code"])
                    if okx_code != "0":
                        okx_msg = data.get("msg", "unknown error")
                        self._metrics.failed_requests += 1
                        self._metrics.last_error = f"OKX {okx_code}: {okx_msg}"
                        self._metrics.last_error_time = datetime.now(UTC)
                        raise IntegrationError(
                            self.name,
                            f"OKX API error {okx_code}: {okx_msg}",
                            code=f"OKX_{okx_code}",
                        )

                    self._metrics.successful_requests += 1
                    self._metrics.total_latency_ms += latency_ms

                    logger.debug("%s API call: %s (latency: %.2fms)", self.name, path, latency_ms)

                    return data

            except aiohttp.ClientError as e:
                self._metrics.failed_requests += 1
                self._metrics.last_error = str(e)
                self._metrics.last_error_time = datetime.now(UTC)
                raise IntegrationError(self.name, str(e), code="NETWORK_ERROR") from e

            except TimeoutError:
                self._metrics.failed_requests += 1
                self._metrics.last_error = f"Timeout after {self._request_timeout}s"
                self._metrics.last_error_time = datetime.now(UTC)
                raise IntegrationError(
                    self.name,
                    f"Timeout after {self._request_timeout}s",
                    code="TIMEOUT",
                ) from None

    # -------------------------------------------------------------------------
    # Portfolio API
    # -------------------------------------------------------------------------

    @staticmethod
    def _validate_inputs(wallet_address: str, chain: str) -> tuple[str, str]:
        """Validate and normalize wallet_address and chain inputs."""
        addr = wallet_address.strip() if wallet_address else ""
        ch = chain.strip().lower() if chain else ""
        if not addr:
            raise ValueError("wallet_address must not be empty")
        if not ch:
            raise ValueError("chain must not be empty")
        return addr, ch

    @staticmethod
    def _cache_address(wallet_address: str, chain: str) -> str:
        """Normalize wallet address for cache keys. Preserves case for Solana (base58)."""
        if chain.lower() in {"solana", "501"}:
            return wallet_address
        return wallet_address.lower()

    async def get_wallet_portfolio(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Get total USD portfolio value for a wallet on a chain."""
        wallet_address, chain = self._validate_inputs(wallet_address, chain)
        cache_key = f"okx:portfolio:{self._cache_address(wallet_address, chain)}:{chain.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return dataclasses.replace(cached, cache_hit=True)

        chain_id = self._CHAIN_IDS.get(chain.lower())
        if not chain_id:
            logger.warning("OKX: unsupported chain %s", chain)
            chain_id = chain

        data = await self._fetch(
            "/api/v6/dex/balance/total-value-by-address",
            params={"address": wallet_address, "chains": chain_id},
        )

        snapshot = self._normalize_total_value(wallet_address, chain, data)
        self._update_cache(cache_key, snapshot)
        return snapshot

    async def get_wallet_positions(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Get token balances + DeFi positions for a wallet on a chain.

        Combines two API calls:
        1. Market Balance API — bare token holdings
        2. Wallet DeFi API — LP, lending, staking, farming positions
        """
        wallet_address, chain = self._validate_inputs(wallet_address, chain)
        cache_key = f"okx:positions:{self._cache_address(wallet_address, chain)}:{chain.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return dataclasses.replace(cached, cache_hit=True)

        chain_id = self._CHAIN_IDS.get(chain.lower())
        if not chain_id:
            logger.warning("OKX: unsupported chain %s", chain)
            chain_id = chain

        # Fetch token balances
        token_data = await self._fetch(
            "/api/v6/dex/balance/all-token-balances-by-address",
            params={"address": wallet_address, "chains": chain_id},
        )
        token_snapshot = self._normalize_token_balances(wallet_address, chain, token_data)

        # Fetch DeFi positions (best-effort — don't fail the whole call on network errors)
        defi_positions: list[WalletPosition] = []
        defi_failed = False
        try:
            defi_positions = await self._fetch_defi_positions(wallet_address, chain_id)
        except (IntegrationError, aiohttp.ClientError, TimeoutError) as e:
            logger.warning("OKX DeFi positions fetch failed for %s on %s: %s", wallet_address, chain, e)
            defi_failed = True

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
        # Use short TTL (10s) when DeFi data is missing to allow quick retry
        cache_ttl = 10 if defi_failed else None
        self._update_cache(cache_key, snapshot, ttl=cache_ttl)
        return snapshot

    async def get_token_balances(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Get only bare token balances (no DeFi positions)."""
        wallet_address, chain = self._validate_inputs(wallet_address, chain)
        chain_id = self._CHAIN_IDS.get(chain.lower(), chain)
        data = await self._fetch(
            "/api/v6/dex/balance/all-token-balances-by-address",
            params={"address": wallet_address, "chains": chain_id},
        )
        return self._normalize_token_balances(wallet_address, chain, data)

    # -------------------------------------------------------------------------
    # DeFi API (Wallet module)
    # -------------------------------------------------------------------------

    async def get_defi_positions(self, wallet_address: str, chain: str) -> WalletPortfolioSnapshot:
        """Get DeFi protocol positions for a wallet on a chain."""
        wallet_address, chain = self._validate_inputs(wallet_address, chain)
        cache_key = f"okx:defi:{self._cache_address(wallet_address, chain)}:{chain.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return dataclasses.replace(cached, cache_hit=True)

        chain_id = self._CHAIN_IDS.get(chain.lower(), chain)
        positions = await self._fetch_defi_positions(wallet_address, chain_id)
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

    async def _fetch_defi_positions(self, wallet_address: str, chain_id: str) -> list[WalletPosition]:
        """Fetch DeFi positions via platform/list then platform/detail."""
        # Step 1: Get list of protocols where wallet has positions
        body = {"walletAddressList": [{"chainIndex": chain_id, "walletAddress": wallet_address}]}
        platform_data = await self._fetch(
            "/api/v6/defi/user/asset/platform/list",
            method="POST",
            json_data=body,
        )

        platforms = self._extract_platforms(platform_data, chain_id)
        if not platforms:
            return []

        # Step 2: Get detailed positions for all protocols in one call
        detail_body = {
            "walletAddressList": [{"chainIndex": chain_id, "walletAddress": wallet_address}],
            "platformList": [{"chainIndex": chain_id, "analysisPlatformId": p["id"]} for p in platforms],
        }
        detail_data = await self._fetch(
            "/api/v6/defi/user/asset/platform/detail",
            method="POST",
            json_data=detail_body,
        )

        return self._normalize_defi_details(detail_data, platforms)

    # -------------------------------------------------------------------------
    # Response normalization
    # -------------------------------------------------------------------------

    def _normalize_total_value(self, wallet_address: str, chain: str, payload: Any) -> WalletPortfolioSnapshot:
        """Normalize the total-value-by-address response."""
        total_value = "0"
        data_list = self._extract_data(payload)
        if data_list:
            total_value = str(self._safe_decimal(data_list[0].get("totalValue", "0")))

        return WalletPortfolioSnapshot(
            provider=self.name,
            wallet_address=wallet_address,
            chain=chain,
            total_value_usd=total_value,
            positions=[],
            cache_hit=False,
        )

    def _normalize_token_balances(self, wallet_address: str, chain: str, payload: Any) -> WalletPortfolioSnapshot:
        """Normalize the all-token-balances-by-address response."""
        data_list = self._extract_token_assets(payload)
        positions: list[WalletPosition] = []
        total = Decimal("0")

        for item in data_list:
            if not isinstance(item, dict):
                continue

            symbol = item.get("symbol", "UNKNOWN")
            token_address = item.get("tokenContractAddress", "")
            balance = item.get("balance", "0")
            token_price = item.get("tokenPrice", "0")
            is_risk = item.get("isRiskToken", False)

            value_usd = self._calc_usd_value(balance, token_price)
            total += self._safe_decimal(value_usd)

            position_id_part = token_address if token_address else "native"
            positions.append(
                WalletPosition(
                    position_id=f"okx:{position_id_part}",
                    protocol="wallet",
                    label=symbol,
                    position_type="token",
                    value_usd=value_usd,
                    pool_address=token_address,
                    token_symbols=[symbol] if symbol else [],
                    details={
                        "balance": balance,
                        "token_price": token_price,
                        "is_risk_token": is_risk,
                        "chain_index": item.get("chainIndex", ""),
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
    def _extract_platforms(payload: Any, chain_id: str) -> list[dict[str, str]]:
        """Extract protocol list from platform/list response.

        Returns list of {"id": analysisPlatformId, "name": platformName}.
        The response can have data as a dict or a list.
        """
        results: list[dict[str, str]] = []

        if not isinstance(payload, dict):
            return results
        data = payload.get("data")

        # data can be a dict (real API) or a list (some responses)
        entries: list[dict[str, Any]] = []
        if isinstance(data, dict):
            entries = [data]
        elif isinstance(data, list):
            entries = [item for item in data if isinstance(item, dict)]

        for entry in entries:
            wallet_platforms = entry.get("walletIdPlatformList")
            if not isinstance(wallet_platforms, list):
                continue
            for wp in wallet_platforms:
                if not isinstance(wp, dict):
                    continue
                platform_list = wp.get("platformList")
                if not isinstance(platform_list, list):
                    continue
                for platform in platform_list:
                    if not isinstance(platform, dict):
                        continue
                    pid = platform.get("analysisPlatformId")
                    pname = platform.get("platformName", "unknown")
                    if pid is not None:
                        results.append({"id": str(pid), "name": str(pname)})
        return results

    @staticmethod
    def _normalize_defi_details(payload: Any, platforms: list[dict[str, str]]) -> list[WalletPosition]:
        """Normalize the platform/detail response into WalletPositions.

        The real API response nests data as:
        data[].walletIdPlatformDetailList[].networkHoldVoList[].investTokenBalanceVoList[]
        Each investment has positionList[] with assetsTokenList[] for token details
        and unclaimFeesDefiTokenInfo[] for unclaimed rewards.

        This top-level function only walks levels 0-2 (wallet entry -> platform
        detail -> network hold) and builds an :class:`OkxDefiContext` for each
        (platform, chain) pair. Row construction at level 3 (investments,
        rewards) is delegated to dedicated extractors. See Phase 5f refactor
        plan.
        """
        platform_names = {p["id"]: p["name"] for p in platforms}
        data_list = _extract_data_entries(payload)
        positions: list[WalletPosition] = []

        for entry in data_list:
            detail_list = entry.get("walletIdPlatformDetailList")
            if not isinstance(detail_list, list):
                continue
            for detail in detail_list:
                if not isinstance(detail, dict):
                    continue
                platform_id = str(detail.get("analysisPlatformId", ""))

                network_holds = detail.get("networkHoldVoList")
                if not isinstance(network_holds, list):
                    continue
                for network_hold in network_holds:
                    if not isinstance(network_hold, dict):
                        continue

                    invest_list = network_hold.get("investTokenBalanceVoList")
                    if not isinstance(invest_list, list):
                        # Preserve byte-for-byte: when invest list is absent,
                        # ``continue`` skips network-level availableRewards too.
                        # Issue #1708 notes the duplication; don't fix here.
                        continue

                    ctx = OkxDefiContext(
                        platform_names=platform_names,
                        platform_id=platform_id,
                        chain_index=network_hold.get("chainIndex", ""),
                    )

                    for invest in invest_list:
                        if not isinstance(invest, dict):
                            continue
                        positions.extend(_extract_position_rows(invest, ctx))
                        positions.extend(_extract_reward_rows_from_position(invest, ctx))

                    positions.extend(_extract_reward_rows_from_network(network_hold.get("availableRewards"), ctx))

        return positions

    @staticmethod
    def _sum_position_values(position_list: Any) -> str:
        """Sum currencyAmount from all assets in positionList."""
        if not isinstance(position_list, list):
            return "0"
        total = Decimal("0")
        for pos in position_list:
            if not isinstance(pos, dict):
                continue
            assets = pos.get("assetsTokenList")
            if isinstance(assets, list):
                for asset in assets:
                    if isinstance(asset, dict):
                        total += OkxIntegration._safe_decimal(asset.get("currencyAmount", "0"))
        return str(total)

    @staticmethod
    def _extract_data(payload: Any) -> list[dict[str, Any]]:
        """Extract the data array from OKX response envelope.

        OKX responses follow: {"code": "0", "msg": "success", "data": [...]}
        """
        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

    @staticmethod
    def _extract_token_assets(payload: Any) -> list[dict[str, Any]]:
        """Extract token assets from the all-token-balances response.

        The token balances endpoint nests tokens inside:
        {"data": [{"tokenAssets": [...]}]}
        """
        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            return []
        first = data[0]
        if isinstance(first, dict):
            assets = first.get("tokenAssets")
            if isinstance(assets, list):
                return [item for item in assets if isinstance(item, dict)]
        return []

    @staticmethod
    def _calc_usd_value(balance: str, token_price: str) -> str:
        """Calculate USD value from human-readable balance and price."""
        bal = OkxIntegration._safe_decimal(balance)
        price = OkxIntegration._safe_decimal(token_price)
        return str(bal * price)

    @staticmethod
    def _safe_decimal(value: Any) -> Decimal:
        if value is None:
            return Decimal("0")
        try:
            parsed = Decimal(str(value))
            return parsed if parsed.is_finite() else Decimal("0")
        except (InvalidOperation, ValueError, TypeError):
            return Decimal("0")
