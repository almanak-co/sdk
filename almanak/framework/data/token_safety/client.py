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
import time
from typing import Any

import aiohttp

from almanak.config import FrameworkConfig, load_config

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
        framework_config: FrameworkConfig | None = None,
    ) -> None:
        self._request_timeout = request_timeout
        self._cache_ttl = cache_ttl
        config = framework_config or load_config().framework
        self._rugcheck_api_key = rugcheck_api_key or config.rugcheck_api_key
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

        token_data = _find_goplus_token_data(data.get("result", {}), mint_address)
        if token_data is None:
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
        has_transfer_fee = _has_nonzero_goplus_transfer_fee(token_data.get("transfer_fee", {}))

        # Transfer hook check
        has_hook = bool(token_data.get("transfer_hook", []))

        # Non-transferable
        non_transferable = str(token_data.get("non_transferable", "0")) == "1"

        # Default account state (2 = frozen)
        default_state = int(token_data.get("default_account_state", "1") or "1")
        default_frozen = default_state == 2

        # Trusted token
        trusted = int(token_data.get("trusted_token", 0) or 0) == 1

        # Holder stats
        holder_count = int(token_data.get("holder_count", 0) or 0)
        top_holder_pct = _goplus_top_holder_pct(token_data.get("holders", []))

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

    def _aggregate_results(  # noqa: C901
        self,
        mint_address: str,
        rugcheck: RugCheckResult | None,
        goplus: GoPlusResult | None,
    ) -> TokenSafetyResult:
        """Combine RugCheck and GoPlus results into a unified assessment."""
        flags: list[RiskFlag] = []
        sources: list[str] = []
        risk_levels: list[RiskLevel] = []

        if rugcheck is not None:
            sources.append("rugcheck")
            rugcheck_flags, rugcheck_levels = _rugcheck_flags_and_levels(rugcheck)
            flags.extend(rugcheck_flags)
            risk_levels.extend(rugcheck_levels)

        if goplus is not None:
            sources.append("goplus")
            goplus_flags, goplus_levels = _goplus_flags_and_levels(goplus)
            flags.extend(goplus_flags)
            risk_levels.extend(goplus_levels)

        overall = _overall_risk_level(risk_levels, goplus)

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


_RISK_LEVEL_ORDER = {
    RiskLevel.SAFE: 0,
    RiskLevel.LOW: 1,
    RiskLevel.MEDIUM: 2,
    RiskLevel.HIGH: 3,
    RiskLevel.CRITICAL: 4,
    RiskLevel.UNKNOWN: 2,
}

_GOPLUS_BOOLEAN_FLAG_SPECS = (
    (
        "mintable",
        "mint_authority_enabled",
        "Token supply can be increased by mint authority",
        RiskLevel.HIGH,
    ),
    (
        "freezable",
        "freeze_authority_enabled",
        "Token accounts can be frozen by authority",
        RiskLevel.HIGH,
    ),
    (
        "closable",
        "close_authority_enabled",
        "Token program can be closed (destroying all tokens)",
        RiskLevel.CRITICAL,
    ),
    (
        "balance_mutable",
        "balance_mutable",
        "Authority can modify token balances directly",
        RiskLevel.CRITICAL,
    ),
    (
        "has_transfer_fee",
        "transfer_fee",
        "Token has a non-zero transfer fee (potential sell tax)",
        RiskLevel.HIGH,
    ),
    (
        "transfer_hook",
        "transfer_hook_active",
        "External transfer hook attached (can block/modify transfers)",
        RiskLevel.HIGH,
    ),
    (
        "non_transferable",
        "non_transferable",
        "Token is soulbound / non-transferable",
        RiskLevel.CRITICAL,
    ),
    (
        "default_account_state_frozen",
        "default_account_frozen",
        "New token accounts start in frozen state",
        RiskLevel.HIGH,
    ),
)


def _rugcheck_flags_and_levels(rugcheck: RugCheckResult) -> tuple[list[RiskFlag], list[RiskLevel]]:
    """Build RugCheck-derived flags and risk levels."""
    flags = list(rugcheck.risks)
    risk_levels: list[RiskLevel] = []

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

    risk_levels.append(_rugcheck_score_to_level(rugcheck.score))
    return flags, risk_levels


def _goplus_boolean_flags(goplus: GoPlusResult) -> tuple[list[RiskFlag], list[RiskLevel]]:
    """Build GoPlus flags for simple boolean capability checks."""
    flags: list[RiskFlag] = []
    risk_levels: list[RiskLevel] = []

    for field_name, flag_name, description, risk_level in _GOPLUS_BOOLEAN_FLAG_SPECS:
        if getattr(goplus, field_name):
            flags.append(
                RiskFlag(
                    name=flag_name,
                    description=description,
                    level=risk_level,
                    source="goplus",
                )
            )
            risk_levels.append(risk_level)

    return flags, risk_levels


def _goplus_holder_flags(goplus: GoPlusResult) -> tuple[list[RiskFlag], list[RiskLevel]]:
    """Build GoPlus holder-count and concentration flags."""
    flags: list[RiskFlag] = []
    risk_levels: list[RiskLevel] = []

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

    return flags, risk_levels


def _goplus_flags_and_levels(goplus: GoPlusResult) -> tuple[list[RiskFlag], list[RiskLevel]]:
    """Build all GoPlus-derived flags and risk levels."""
    flags, risk_levels = _goplus_boolean_flags(goplus)
    holder_flags, holder_levels = _goplus_holder_flags(goplus)
    flags.extend(holder_flags)
    risk_levels.extend(holder_levels)

    if goplus.trusted_token:
        risk_levels.append(RiskLevel.SAFE)

    return flags, risk_levels


def _overall_risk_level(risk_levels: list[RiskLevel], goplus: GoPlusResult | None) -> RiskLevel:
    """Select the final risk level with the GoPlus trusted-token cap."""
    if not risk_levels:
        return RiskLevel.UNKNOWN

    overall = max(risk_levels, key=lambda lvl: _RISK_LEVEL_ORDER.get(lvl, 2))
    is_trusted = goplus is not None and goplus.trusted_token
    if is_trusted and _RISK_LEVEL_ORDER.get(overall, 0) > _RISK_LEVEL_ORDER[RiskLevel.MEDIUM]:
        return RiskLevel.MEDIUM
    return overall


def _goplus_status(field: Any) -> bool:
    """Parse a GoPlus authority field. Returns True if the capability exists (risky)."""
    if field is None:
        return False
    if isinstance(field, dict):
        status = field.get("status", "0")
        return str(status) == "1"
    return str(field) == "1"


def _find_goplus_token_data(result_data: Any, mint_address: str) -> Any | None:
    """Find the GoPlus token payload, preserving legacy lookup semantics."""
    token_data = result_data.get(mint_address, {})
    if token_data:
        return token_data

    for key, val in result_data.items():
        if key.lower() == mint_address.lower():
            token_data = val
            break
    if not token_data:
        return None
    return token_data


def _has_nonzero_goplus_transfer_fee(transfer_fee: Any) -> bool:
    """Return True when GoPlus reports a truthy non-zero transfer fee."""
    if not isinstance(transfer_fee, dict) or not transfer_fee:
        return False

    fee_rate = transfer_fee.get("fee_rate")
    current_rate = transfer_fee.get("current_fee_rate")
    if fee_rate and str(fee_rate) != "0":
        return True
    return bool(current_rate and str(current_rate) != "0")


def _goplus_top_holder_pct(holders: Any) -> float:
    """Parse the top holder percentage, keeping malformed percent values soft."""
    if holders and isinstance(holders, list):
        try:
            return float(holders[0].get("percent", 0)) if holders else 0.0
        except (ValueError, TypeError, IndexError):
            pass
    return 0.0


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
