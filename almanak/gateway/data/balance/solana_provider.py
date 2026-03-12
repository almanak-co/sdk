"""Solana Balance Provider for on-chain balance queries.

This module provides a balance provider for Solana, querying native SOL and
SPL token balances via Solana JSON-RPC.

Key Features:
    - Query native SOL balance via getBalance
    - Query SPL token balances via getTokenAccountsByOwner
    - Handle token decimals correctly (9 for SOL, 6 for USDC, etc.)
    - Cache balances with short TTL (5s) to reduce RPC load
    - RPC error handling with retry and clear error messages

Example:
    from almanak.gateway.data.balance.solana_provider import SolanaBalanceProvider

    provider = SolanaBalanceProvider(
        rpc_url="https://api.mainnet-beta.solana.com",
        wallet_address="7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
        chain="solana",
    )

    # Query SOL balance
    result = await provider.get_native_balance()
    print(f"SOL Balance: {result.balance}")

    # Query USDC balance
    result = await provider.get_balance("USDC")
    print(f"USDC Balance: {result.balance}")
"""

import asyncio
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import aiohttp

from almanak.framework.data.interfaces import (
    BalanceResult,
    DataSourceError,
    DataSourceUnavailable,
)

logger = logging.getLogger(__name__)

# SOL has 9 decimal places (lamports)
SOL_DECIMALS = 9

# Native SOL placeholder address (System Program)
SOL_NATIVE_ADDRESS = "11111111111111111111111111111111"

# SPL Token Program ID
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

# Token-2022 Program ID (some newer tokens use this)
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"


class SolanaBalanceProvider:
    """On-chain balance provider for Solana using JSON-RPC.

    Implements the same interface pattern as Web3BalanceProvider but uses
    Solana's JSON-RPC API for balance queries.
    """

    def __init__(
        self,
        rpc_url: str,
        wallet_address: str,
        chain: str = "solana",
        cache_ttl: int = 5,
        request_timeout: float = 10.0,
        max_retries: int = 3,
        retry_delay: float = 0.5,
    ) -> None:
        self._rpc_url = rpc_url
        self._wallet_address = wallet_address
        self._chain = chain.lower()
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._session: aiohttp.ClientSession | None = None

        # Balance cache: token_key -> (BalanceResult, cached_at)
        self._cache: dict[str, tuple[BalanceResult, datetime]] = {}

        # Token resolver (lazy)
        self._token_resolver: Any = None

        logger.info(
            "Initialized SolanaBalanceProvider",
            extra={
                "rpc_url": rpc_url[:40] + "...",
                "wallet": wallet_address[:10] + "...",
                "chain": chain,
            },
        )

    def _get_token_resolver(self):
        """Lazy-load token resolver."""
        if self._token_resolver is None:
            from almanak.framework.data.tokens import get_token_resolver

            self._token_resolver = get_token_resolver()
        return self._token_resolver

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._request_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def _rpc_call(self, method: str, params: list) -> Any:
        """Make a Solana JSON-RPC call with retry.

        Args:
            method: RPC method name (e.g., "getBalance")
            params: RPC parameters

        Returns:
            The "result" field from the JSON-RPC response

        Raises:
            DataSourceUnavailable: If all retries fail
        """
        session = await self._get_session()
        last_error: Exception | None = None

        for attempt in range(self._max_retries):
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": method,
                    "params": params,
                }
                async with session.post(
                    self._rpc_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise DataSourceError(f"HTTP {response.status}: {error_text}")

                    data = await response.json()

                    if "error" in data:
                        raise DataSourceError(f"RPC error: {data['error']}")

                    return data.get("result")

            except (aiohttp.ClientError, TimeoutError) as e:
                last_error = e
                logger.warning(
                    "Solana RPC %s failed (attempt %d/%d): %s",
                    method,
                    attempt + 1,
                    self._max_retries,
                    str(e),
                )
            except DataSourceError:
                raise
            except Exception as e:
                last_error = e
                logger.warning(
                    "Solana RPC %s error (attempt %d/%d): %s",
                    method,
                    attempt + 1,
                    self._max_retries,
                    str(e),
                )

            if attempt < self._max_retries - 1:
                wait_time = self._retry_delay * (2**attempt)
                await asyncio.sleep(wait_time)

        raise DataSourceUnavailable(
            source="solana_balance_provider",
            reason=f"RPC call {method} failed after {self._max_retries} attempts: {last_error}",
        )

    def _get_cached(self, token_key: str) -> BalanceResult | None:
        """Get cached entry if not expired."""
        entry = self._cache.get(token_key)
        if entry is None:
            return None
        result, cached_at = entry
        age = (datetime.now(UTC) - cached_at).total_seconds()
        if age > self._cache_ttl:
            return None
        return result

    async def get_native_balance(self) -> BalanceResult:
        """Get native SOL balance.

        Returns:
            BalanceResult for SOL
        """
        cache_key = "SOL"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        start_time = time.time()
        result_data = await self._rpc_call("getBalance", [self._wallet_address])
        latency_ms = (time.time() - start_time) * 1000

        # getBalance returns {"context": {...}, "value": lamports}
        lamports = result_data.get("value", 0) if isinstance(result_data, dict) else 0
        balance = Decimal(lamports) / Decimal(10**SOL_DECIMALS)

        result = BalanceResult(
            balance=balance,
            token="SOL",
            address=SOL_NATIVE_ADDRESS,
            decimals=SOL_DECIMALS,
            raw_balance=lamports,
            timestamp=datetime.now(UTC),
            stale=False,
        )

        self._cache[cache_key] = (result, datetime.now(UTC))
        logger.debug("Fetched SOL balance: %s (%.2fms)", balance, latency_ms)
        return result

    async def get_balance(self, token: str) -> BalanceResult:
        """Get balance for a token (native SOL or SPL token).

        Args:
            token: Token symbol (e.g., "SOL", "USDC") or mint address

        Returns:
            BalanceResult with balance in human-readable units
        """
        token_upper = token.upper()

        # Native SOL
        if token_upper == "SOL":
            return await self.get_native_balance()

        cache_key = token_upper
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # Resolve mint address and decimals
        resolver = self._get_token_resolver()
        try:
            resolved = resolver.resolve(token, self._chain)
            mint_address = resolved.address
            decimals = resolved.decimals
        except Exception:
            # If token looks like a Solana address (base58, 32-44 chars), try it directly
            if len(token) >= 32 and not token.startswith("0x"):
                mint_address = token
                decimals = None  # Will try to get from account data
            else:
                raise DataSourceUnavailable(
                    source="solana_balance_provider",
                    reason=f"Cannot resolve token '{token}' on {self._chain}",
                ) from None

        start_time = time.time()

        # Query all token accounts for this mint owned by the wallet
        raw_balance, resolved_decimals = await self._get_spl_token_balance(mint_address, decimals)
        latency_ms = (time.time() - start_time) * 1000

        if resolved_decimals is None:
            raise DataSourceUnavailable(
                source="solana_balance_provider",
                reason=f"Cannot determine decimals for token '{token}'",
            )

        balance = Decimal(raw_balance) / Decimal(10**resolved_decimals)

        result = BalanceResult(
            balance=balance,
            token=token_upper,
            address=mint_address,
            decimals=resolved_decimals,
            raw_balance=raw_balance,
            timestamp=datetime.now(UTC),
            stale=False,
        )

        self._cache[cache_key] = (result, datetime.now(UTC))
        logger.debug("Fetched %s balance: %s (%.2fms)", token_upper, balance, latency_ms)
        return result

    async def _get_spl_token_balance(self, mint_address: str, known_decimals: int | None) -> tuple[int, int | None]:
        """Get SPL token balance for a mint address.

        Uses getTokenAccountsByOwner to find all token accounts for the mint,
        then sums balances.

        Args:
            mint_address: SPL token mint address
            known_decimals: Token decimals if known, None to read from account

        Returns:
            Tuple of (total_raw_balance, decimals)
        """
        # getTokenAccountsByOwner with {"mint": ...} filter checks both
        # Token Program and Token-2022 automatically
        result = await self._rpc_call(
            "getTokenAccountsByOwner",
            [
                self._wallet_address,
                {"mint": mint_address},
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                },
            ],
        )

        accounts = result.get("value", []) if isinstance(result, dict) else []

        if accounts:
            total_balance = 0
            decimals = known_decimals
            for account in accounts:
                parsed = account.get("account", {}).get("data", {}).get("parsed", {})
                info = parsed.get("info", {})
                token_amount = info.get("tokenAmount", {})

                amount_str = token_amount.get("amount", "0")
                total_balance += int(amount_str)

                if decimals is None:
                    decimals = token_amount.get("decimals")

            return total_balance, decimals

        # No accounts found - wallet holds 0 of this token
        return 0, known_decimals

    def invalidate_cache(self, token: str | None = None) -> None:
        """Invalidate cached balances.

        Args:
            token: Specific token to invalidate, or None to clear all
        """
        if token is not None:
            token_key = token.upper()
            self._cache.pop(token_key, None)
        else:
            self._cache.clear()

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
        self._cache.clear()

    async def __aenter__(self) -> "SolanaBalanceProvider":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()


__all__ = [
    "SOL_DECIMALS",
    "SOL_NATIVE_ADDRESS",
    "SolanaBalanceProvider",
]
