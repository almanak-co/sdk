"""Token safety client for Solana scam/honeypot detection.

Queries RugCheck and GoPlus APIs to assess whether a Solana token
is safe to trade. Combines results from multiple sources into a
unified TokenSafetyResult with risk scoring.

Both APIs have free tiers:
  - RugCheck: No auth required for /v1/tokens/{mint}/report/summary
  - GoPlus: No auth required, 30 requests/minute

Example::

    async with TokenSafetyClient() as client:
        result = await client.check_token("DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263")
        if result.is_dangerous:
            print(f"SCAM ALERT: {result.flag_names}")
        elif result.is_safe:
            print(f"Token appears safe (score={result.risk_score})")
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

import aiohttp

from .models import (
    GoPlusResult,
    RiskFlag,
    RiskLevel,
    RugCheckResult,
    TokenSafetyResult,
)

logger = logging.getLogger(__name__)

# API base URLs
RUGCHECK_BASE_URL = "https://api.rugcheck.xyz"
GOPLUS_BASE_URL = "https://api.gopluslabs.io"

# Rate limits
_RUGCHECK_RATE_LIMIT = 60 / 30  # ~30 req/min -> 2.0s
_GOPLUS_RATE_LIMIT = 60 / 30  # 30 req/min -> 2.0s

# RugCheck score thresholds (lower = safer)
_RUGCHECK_SAFE_THRESHOLD = 100
_RUGCHECK_LOW_THRESHOLD = 300
_RUGCHECK_MEDIUM_THRESHOLD = 600
_RUGCHECK_HIGH_THRESHOLD = 900


class TokenSafetyError(Exception):
    """Base exception for token safety check errors."""


class TokenSafetyClient:
    """Async client for token safety checks on Solana.

    Queries RugCheck and GoPlus APIs in parallel, aggregates results
    into a unified TokenSafetyResult.

    Args:
        request_timeout: HTTP timeout per request in seconds.
        cache_ttl: Cache TTL in seconds (default 5 min — token properties are stable).
        rugcheck_api_key: Optional RugCheck API key (env: RUGCHECK_API_KEY).
    """

    def __init__(
        self,
        request_timeout: float = 15.0,
        cache_ttl: int = 300,
        rugcheck_api_key: str | None = None,
    ) -> None:
        self._request_timeout = request_timeout
        self._cache_ttl = cache_ttl
        self._rugcheck_api_key = rugcheck_api_key or os.environ.get("RUGCHECK_API_KEY")
        self._session: aiohttp.ClientSession | None = None
        self._cache: dict[str, tuple[float, TokenSafetyResult]] = {}
        self._last_rugcheck_request: float = 0.0
        self._last_goplus_request: float = 0.0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._request_timeout),
            )
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> TokenSafetyClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    async def check_token(self, mint_address: str) -> TokenSafetyResult:
        """Check if a Solana token is safe to trade.

        Queries RugCheck and GoPlus in parallel, aggregates results.
        Results are cached for cache_ttl seconds.

        Args:
            mint_address: Solana token mint address (base58).

        Returns:
            TokenSafetyResult with risk level, score, and flags.
        """
        # Check cache
        cached = self._cache.get(mint_address)
        if cached is not None:
            cached_at, cached_result = cached
            if time.time() - cached_at < self._cache_ttl:
                return cached_result

        # Query both APIs in parallel
        rugcheck_result, goplus_result = await asyncio.gather(
            self._check_rugcheck(mint_address),
            self._check_goplus(mint_address),
            return_exceptions=True,
        )

        # Handle exceptions gracefully
        if isinstance(rugcheck_result, BaseException):
            logger.warning("RugCheck API failed for %s: %s", mint_address[:12], rugcheck_result)
            rugcheck_result = None
        if isinstance(goplus_result, BaseException):
            logger.warning("GoPlus API failed for %s: %s", mint_address[:12], goplus_result)
            goplus_result = None

        # Aggregate results
        result = self._aggregate_results(mint_address, rugcheck_result, goplus_result)

        # Cache
        self._cache[mint_address] = (time.time(), result)
        return result

    async def check_rugcheck(self, mint_address: str) -> RugCheckResult:
        """Query RugCheck API only. Raises on failure."""
        result = await self._check_rugcheck(mint_address)
        if result is None:
            raise TokenSafetyError(f"RugCheck returned no data for {mint_address}")
        return result

    async def check_goplus(self, mint_address: str) -> GoPlusResult:
        """Query GoPlus API only. Raises on failure."""
        result = await self._check_goplus(mint_address)
        if result is None:
            raise TokenSafetyError(f"GoPlus returned no data for {mint_address}")
        return result

    # -----------------------------------------------------------------------
    # RugCheck API
    # -----------------------------------------------------------------------

    async def _check_rugcheck(self, mint_address: str) -> RugCheckResult | None:
        """Query RugCheck full token report.

        Endpoint: GET /v1/tokens/{mint}/report
        Full report includes rugged flag, token metadata, liquidity, and holder data.
        No auth required. Retries up to 2 times on rate-limit (429).
        """
        session = await self._get_session()
        url = f"{RUGCHECK_BASE_URL}/v1/tokens/{mint_address}/report"

        headers: dict[str, str] = {}
        if self._rugcheck_api_key:
            headers["X-API-KEY"] = self._rugcheck_api_key

        max_retries = 2
        for attempt in range(max_retries + 1):
            # Rate limit
            elapsed = time.time() - self._last_rugcheck_request
            if elapsed < _RUGCHECK_RATE_LIMIT:
                await asyncio.sleep(_RUGCHECK_RATE_LIMIT - elapsed)

            try:
                async with session.get(url, headers=headers) as response:
                    self._last_rugcheck_request = time.time()

                    if response.status == 429:
                        if attempt < max_retries:
                            backoff = _RUGCHECK_RATE_LIMIT * (attempt + 2)
                            logger.info(
                                "RugCheck rate limited, retrying in %.1fs (attempt %d/%d)",
                                backoff,
                                attempt + 1,
                                max_retries,
                            )
                            await asyncio.sleep(backoff)
                            continue
                        logger.warning("RugCheck rate limited after %d retries", max_retries)
                        return None

                    if response.status != 200:
                        text = await response.text()
                        logger.warning("RugCheck HTTP %d: %s", response.status, text[:200])
                        return None

                    data = await response.json()
                    return self._parse_rugcheck_response(data)

            except aiohttp.ClientError as e:
                logger.warning("RugCheck request failed: %s", e)
                return None

        return None

    def _parse_rugcheck_response(self, data: dict[str, Any]) -> RugCheckResult:
        """Parse a RugCheck full report response into RugCheckResult."""
        # Extract risk flags
        risks: list[RiskFlag] = []
        for risk in data.get("risks", []) or []:
            risk_name = risk.get("name", "unknown")
            risk_desc = risk.get("description", risk.get("value", ""))
            risk_level_str = risk.get("level", "").lower()
            level = _parse_risk_level(risk_level_str)
            risks.append(RiskFlag(name=risk_name, description=str(risk_desc), level=level, source="rugcheck"))

        # Token metadata — full report has `tokenMeta` and `fileMeta`
        meta = data.get("tokenMeta", {}) or {}
        file_meta = data.get("fileMeta", {}) or {}

        # Score and derive risk level from score thresholds
        score = int(data.get("score", 0))
        risk_level = _rugcheck_score_to_level(score).value

        return RugCheckResult(
            score=score,
            risk_level=risk_level,
            risks=risks,
            rugged=bool(data.get("rugged", False)),
            token_name=file_meta.get("name", meta.get("name", "")),
            token_symbol=meta.get("symbol", file_meta.get("symbol", "")),
            total_market_liquidity=_safe_float(data.get("totalMarketLiquidity")),
            raw_response=data,
        )

    # -----------------------------------------------------------------------
    # GoPlus API
    # -----------------------------------------------------------------------

    async def _check_goplus(self, mint_address: str) -> GoPlusResult | None:
        """Query GoPlus Solana token security API.

        Endpoint: GET /api/v1/solana/token_security?contract_addresses={address}
        No authentication required (free tier: 30 req/min).
        Retries up to 2 times on rate-limit (429) with exponential backoff.
        """
        session = await self._get_session()
        url = f"{GOPLUS_BASE_URL}/api/v1/solana/token_security"
        params = {"contract_addresses": mint_address}

        max_retries = 2
        for attempt in range(max_retries + 1):
            # Rate limit
            elapsed = time.time() - self._last_goplus_request
            if elapsed < _GOPLUS_RATE_LIMIT:
                await asyncio.sleep(_GOPLUS_RATE_LIMIT - elapsed)

            try:
                async with session.get(url, params=params) as response:
                    self._last_goplus_request = time.time()

                    if response.status == 429:
                        if attempt < max_retries:
                            backoff = _GOPLUS_RATE_LIMIT * (attempt + 2)
                            logger.info(
                                "GoPlus rate limited, retrying in %.1fs (attempt %d/%d)",
                                backoff,
                                attempt + 1,
                                max_retries,
                            )
                            await asyncio.sleep(backoff)
                            continue
                        logger.warning("GoPlus rate limited after %d retries", max_retries)
                        return None

                    if response.status != 200:
                        text = await response.text()
                        logger.warning("GoPlus HTTP %d: %s", response.status, text[:200])
                        return None

                    data = await response.json()
                    return self._parse_goplus_response(data, mint_address)

            except aiohttp.ClientError as e:
                logger.warning("GoPlus request failed: %s", e)
                return None

        return None

    def _parse_goplus_response(self, data: dict[str, Any], mint_address: str) -> GoPlusResult | None:
        """Parse a GoPlus Solana token security response."""
        if data.get("code") != 1:
            logger.warning("GoPlus returned error: %s", data.get("message"))
            return None

        result_data = data.get("result", {})
        # GoPlus keys by mint address
        token_data = result_data.get(mint_address, {})
        if not token_data:
            # Try case-insensitive match
            for key, val in result_data.items():
                if key.lower() == mint_address.lower():
                    token_data = val
                    break
            if not token_data:
                return None

        # Parse authority-based fields (status "1" = capability exists = risky)
        mintable = _goplus_status(token_data.get("mintable"))
        freezable = _goplus_status(token_data.get("freezable"))
        closable = _goplus_status(token_data.get("closable"))
        balance_mutable = _goplus_status(token_data.get("balance_mutable_authority"))
        transfer_fee_upgradable = _goplus_status(token_data.get("transfer_fee_upgradable"))
        transfer_hook_upgradable = _goplus_status(token_data.get("transfer_hook_upgradable"))
        metadata_mutable = _goplus_status(token_data.get("metadata_mutable"))

        # Transfer fee check
        transfer_fee = token_data.get("transfer_fee", {})
        has_transfer_fee = False
        if isinstance(transfer_fee, dict) and transfer_fee:
            fee_rate = transfer_fee.get("fee_rate")
            current_rate = transfer_fee.get("current_fee_rate")
            if fee_rate and str(fee_rate) != "0":
                has_transfer_fee = True
            elif current_rate and str(current_rate) != "0":
                has_transfer_fee = True

        # Transfer hook check
        transfer_hook = token_data.get("transfer_hook", [])
        has_hook = bool(transfer_hook) if isinstance(transfer_hook, list) else bool(transfer_hook)

        # Non-transferable
        non_transferable = str(token_data.get("non_transferable", "0")) == "1"

        # Default account state (2 = frozen)
        default_state = int(token_data.get("default_account_state", "1") or "1")
        default_frozen = default_state == 2

        # Trusted token
        trusted = int(token_data.get("trusted_token", 0) or 0) == 1

        # Holder stats
        holder_count = int(token_data.get("holder_count", 0) or 0)
        top_holder_pct = 0.0
        holders = token_data.get("holders", [])
        if holders and isinstance(holders, list):
            try:
                top_holder_pct = float(holders[0].get("percent", 0)) if holders else 0.0
            except (ValueError, TypeError, IndexError):
                pass

        return GoPlusResult(
            mintable=mintable,
            freezable=freezable,
            closable=closable,
            balance_mutable=balance_mutable,
            has_transfer_fee=has_transfer_fee,
            transfer_fee_upgradable=transfer_fee_upgradable,
            transfer_hook=has_hook,
            transfer_hook_upgradable=transfer_hook_upgradable,
            metadata_mutable=metadata_mutable,
            non_transferable=non_transferable,
            default_account_state_frozen=default_frozen,
            trusted_token=trusted,
            holder_count=holder_count,
            top_holder_pct=top_holder_pct,
            raw_response=token_data,
        )

    # -----------------------------------------------------------------------
    # Result aggregation
    # -----------------------------------------------------------------------

    def _aggregate_results(
        self,
        mint_address: str,
        rugcheck: RugCheckResult | None,
        goplus: GoPlusResult | None,
    ) -> TokenSafetyResult:
        """Combine RugCheck and GoPlus results into a unified assessment."""
        flags: list[RiskFlag] = []
        sources: list[str] = []
        risk_levels: list[RiskLevel] = []

        # --- RugCheck flags ---
        if rugcheck is not None:
            sources.append("rugcheck")
            flags.extend(rugcheck.risks)

            if rugcheck.rugged:
                flags.append(
                    RiskFlag(
                        name="already_rugged",
                        description="Token has been confirmed as a rug pull",
                        level=RiskLevel.CRITICAL,
                        source="rugcheck",
                    )
                )
                risk_levels.append(RiskLevel.CRITICAL)

            # Map RugCheck score to risk level
            rc_level = _rugcheck_score_to_level(rugcheck.score)
            risk_levels.append(rc_level)

        # --- GoPlus flags ---
        if goplus is not None:
            sources.append("goplus")

            if goplus.mintable:
                flags.append(
                    RiskFlag(
                        name="mint_authority_enabled",
                        description="Token supply can be increased by mint authority",
                        level=RiskLevel.HIGH,
                        source="goplus",
                    )
                )
                risk_levels.append(RiskLevel.HIGH)

            if goplus.freezable:
                flags.append(
                    RiskFlag(
                        name="freeze_authority_enabled",
                        description="Token accounts can be frozen by authority",
                        level=RiskLevel.HIGH,
                        source="goplus",
                    )
                )
                risk_levels.append(RiskLevel.HIGH)

            if goplus.closable:
                flags.append(
                    RiskFlag(
                        name="close_authority_enabled",
                        description="Token program can be closed (destroying all tokens)",
                        level=RiskLevel.CRITICAL,
                        source="goplus",
                    )
                )
                risk_levels.append(RiskLevel.CRITICAL)

            if goplus.balance_mutable:
                flags.append(
                    RiskFlag(
                        name="balance_mutable",
                        description="Authority can modify token balances directly",
                        level=RiskLevel.CRITICAL,
                        source="goplus",
                    )
                )
                risk_levels.append(RiskLevel.CRITICAL)

            if goplus.has_transfer_fee:
                flags.append(
                    RiskFlag(
                        name="transfer_fee",
                        description="Token has a non-zero transfer fee (potential sell tax)",
                        level=RiskLevel.HIGH,
                        source="goplus",
                    )
                )
                risk_levels.append(RiskLevel.HIGH)

            if goplus.transfer_hook:
                flags.append(
                    RiskFlag(
                        name="transfer_hook_active",
                        description="External transfer hook attached (can block/modify transfers)",
                        level=RiskLevel.HIGH,
                        source="goplus",
                    )
                )
                risk_levels.append(RiskLevel.HIGH)

            if goplus.non_transferable:
                flags.append(
                    RiskFlag(
                        name="non_transferable",
                        description="Token is soulbound / non-transferable",
                        level=RiskLevel.CRITICAL,
                        source="goplus",
                    )
                )
                risk_levels.append(RiskLevel.CRITICAL)

            if goplus.default_account_state_frozen:
                flags.append(
                    RiskFlag(
                        name="default_account_frozen",
                        description="New token accounts start in frozen state",
                        level=RiskLevel.HIGH,
                        source="goplus",
                    )
                )
                risk_levels.append(RiskLevel.HIGH)

            if goplus.holder_count < 100:
                flags.append(
                    RiskFlag(
                        name="low_holder_count",
                        description=f"Very few holders ({goplus.holder_count})",
                        level=RiskLevel.MEDIUM,
                        source="goplus",
                    )
                )
                risk_levels.append(RiskLevel.MEDIUM)

            if goplus.top_holder_pct > 50.0:
                flags.append(
                    RiskFlag(
                        name="concentrated_holdings",
                        description=f"Top holder owns {goplus.top_holder_pct:.1f}% of supply",
                        level=RiskLevel.HIGH,
                        source="goplus",
                    )
                )
                risk_levels.append(RiskLevel.HIGH)
            elif goplus.top_holder_pct > 20.0:
                flags.append(
                    RiskFlag(
                        name="high_holder_concentration",
                        description=f"Top holder owns {goplus.top_holder_pct:.1f}% of supply",
                        level=RiskLevel.MEDIUM,
                        source="goplus",
                    )
                )
                risk_levels.append(RiskLevel.MEDIUM)

            if goplus.trusted_token:
                risk_levels.append(RiskLevel.SAFE)

        # --- Compute overall risk ---
        if not risk_levels:
            overall = RiskLevel.UNKNOWN
        else:
            # Take the worst (highest severity)
            level_order = {
                RiskLevel.SAFE: 0,
                RiskLevel.LOW: 1,
                RiskLevel.MEDIUM: 2,
                RiskLevel.HIGH: 3,
                RiskLevel.CRITICAL: 4,
                RiskLevel.UNKNOWN: 2,  # treat unknown as medium
            }
            overall = max(risk_levels, key=lambda lvl: level_order.get(lvl, 2))

            # For GoPlus-trusted tokens (e.g. USDC, USDT), cap risk at MEDIUM.
            # Authorities like mint/freeze are expected for regulated stablecoins
            # and shouldn't flag them as dangerous.
            is_trusted = goplus is not None and goplus.trusted_token
            if is_trusted and level_order.get(overall, 0) > level_order[RiskLevel.MEDIUM]:
                overall = RiskLevel.MEDIUM

        # Normalized risk score (0.0 = safest, 1.0 = most dangerous)
        risk_score = _compute_risk_score(rugcheck, goplus, flags)

        return TokenSafetyResult(
            token_address=mint_address,
            risk_level=overall,
            risk_score=risk_score,
            flags=flags,
            rugcheck=rugcheck,
            goplus=goplus,
            sources=sources,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _goplus_status(field: Any) -> bool:
    """Parse a GoPlus authority field. Returns True if the capability exists (risky)."""
    if field is None:
        return False
    if isinstance(field, dict):
        status = field.get("status", "0")
        return str(status) == "1"
    return str(field) == "1"


def _parse_risk_level(level_str: str) -> RiskLevel:
    """Parse a risk level string to enum."""
    mapping = {
        "safe": RiskLevel.SAFE,
        "good": RiskLevel.SAFE,
        "low": RiskLevel.LOW,
        "warn": RiskLevel.MEDIUM,
        "medium": RiskLevel.MEDIUM,
        "high": RiskLevel.HIGH,
        "danger": RiskLevel.HIGH,
        "critical": RiskLevel.CRITICAL,
    }
    return mapping.get(level_str.lower(), RiskLevel.MEDIUM)


def _rugcheck_score_to_level(score: int) -> RiskLevel:
    """Convert RugCheck numeric score to RiskLevel."""
    if score <= _RUGCHECK_SAFE_THRESHOLD:
        return RiskLevel.SAFE
    if score <= _RUGCHECK_LOW_THRESHOLD:
        return RiskLevel.LOW
    if score <= _RUGCHECK_MEDIUM_THRESHOLD:
        return RiskLevel.MEDIUM
    if score <= _RUGCHECK_HIGH_THRESHOLD:
        return RiskLevel.HIGH
    return RiskLevel.CRITICAL


def _compute_risk_score(
    rugcheck: RugCheckResult | None,
    goplus: GoPlusResult | None,
    flags: list[RiskFlag],
) -> float:
    """Compute a normalized 0.0-1.0 risk score from all signals."""
    scores: list[float] = []

    # RugCheck score contribution (0-1000+ scale -> 0.0-1.0)
    if rugcheck is not None:
        rc_normalized = min(1.0, rugcheck.score / 1000.0)
        scores.append(rc_normalized)

    # GoPlus critical flags contribution
    if goplus is not None:
        gp_score = 0.0
        if goplus.mintable:
            gp_score += 0.2
        if goplus.freezable:
            gp_score += 0.2
        if goplus.closable:
            gp_score += 0.3
        if goplus.balance_mutable:
            gp_score += 0.3
        if goplus.has_transfer_fee:
            gp_score += 0.25
        if goplus.transfer_hook:
            gp_score += 0.15
        if goplus.non_transferable:
            gp_score += 0.5
        if goplus.trusted_token:
            gp_score -= 0.3
        gp_score = max(0.0, min(1.0, gp_score))
        scores.append(gp_score)

    if not scores:
        return 0.5  # Unknown = middle risk

    return sum(scores) / len(scores)


def _safe_float(val: Any) -> float:
    try:
        return float(val) if val is not None else 0.0
    except (ValueError, TypeError):
        return 0.0
