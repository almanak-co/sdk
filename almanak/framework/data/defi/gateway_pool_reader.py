"""Gateway-backed DEX pool reserve reader (VIB-4845, T3-C).

``MarketSnapshot.pool_reserves(...)`` returns a :class:`PoolReserves`
snapshot of a concentrated-liquidity pool — token0/token1 metadata,
human-readable reserves, fee tier, sqrtPriceX96, tick, and in-range
liquidity. The legacy :class:`almanak.framework.data.defi.pools.UniswapV3PoolReader`
that satisfies the same shape opens its own direct ``AsyncWeb3`` HTTP-provider
connections — a gateway-boundary violation that can only run in tests or
boundary-exempt connector internals, never in the strategy container.

This module provides the boundary-compliant equivalent: all chain reads go
through the gateway ``eth_call`` proxy (the ``rpc_call`` closure the builder
wires from ``gateway_client.eth_call``), reusing the pure decode helpers that
already back the gateway-backed price reader
(:mod:`almanak.framework.data.pools.reader`). No new gateway service is
required — slot0 / liquidity / token0 / token1 / fee / balanceOf / decimals are
all plain ``eth_call`` reads.

Token symbol / name metadata is resolved through the registry-backed
:class:`TokenResolver` (``get_token_resolver()`` — no egress). Decimals fall
back to an on-chain ``decimals()`` read through the same gateway proxy when the
registry has no record, so a non-registered token never silently mis-scales the
reserves (Empty != Zero).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.interfaces import DataSourceError
from almanak.framework.data.pools.reader import (
    FEE_SELECTOR,
    LIQUIDITY_SELECTOR,
    SLOT0_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    decode_address,
    decode_slot0,
    decode_uint,
)

from .pools import PoolReserves

if TYPE_CHECKING:
    from ..tokens.models import ChainToken

# ERC-20 selectors used for reserve reads.
#   decimals()          -> 0x313ce567
#   balanceOf(address)  -> 0x70a08231 + 32-byte left-padded address
_DECIMALS_SELECTOR = "0x313ce567"
_BALANCE_OF_SELECTOR = "0x70a08231"

# Upper bound on a sane ERC-20 ``decimals()``. ``decode_uint`` accepts any
# 32-byte word, so a malformed / malicious ``decimals()`` response could feed an
# astronomically large exponent into ``10**decimals`` (a huge-int allocation /
# DoS) instead of a typed failure. Real tokens sit far below this; the few
# high-precision tokens (e.g. some rebasing / yield wrappers) stay under ~30.
_MAX_REASONABLE_DECIMALS = 36

# Synthetic symbol stamped when the registry has no record for a token. TVL
# cannot be priced for an unresolved symbol, so it is skipped (best-effort).
_UNKNOWN_SYMBOL = "UNKNOWN"

_LOG = logging.getLogger(__name__)


def _is_valid_decimals(value: Any) -> bool:
    """True if ``value`` is int-like within the sane ERC-20 range (0..36).

    Applied to BOTH the on-chain ``decimals()`` read and the registry-provided
    value: a bad decimals from either source (a malicious eth_call word, or a
    wrong registry record like the historical BTCB/WBTC 8-vs-18 mixup) would
    mis-scale reserves or feed ``10**decimals`` a huge int.
    """
    try:
        decimals = int(value)
    except (TypeError, ValueError):
        return False
    return 0 <= decimals <= _MAX_REASONABLE_DECIMALS


class GatewayPoolReserveReader:
    """Reads Uniswap-V3-style pool reserves through the gateway eth_call proxy.

    The reader is intentionally protocol-shaped like the legacy
    ``UniswapV3PoolReader`` (async ``get_pool_reserves(pool_address, chain)``)
    so ``MarketSnapshot.pool_reserves`` can call it without change, but it never
    opens a socket itself — every read is an ``eth_call`` through the gateway.

    Args:
        rpc_call: ``Callable(chain, to_address, calldata_hex) -> bytes`` — the
            sanctioned gateway eth_call proxy (the same closure the builder
            wires for the price providers).
        token_resolver: Optional registry-backed ``TokenResolver`` for token
            symbol / name / decimals lookups. When ``None`` (or a lookup
            misses) the reader falls back to an on-chain ``decimals()`` read and
            a synthetic ``UNKNOWN`` symbol.
        price_oracle: Optional framework ``PriceOracle`` used to compute
            ``tvl_usd`` via ``get_aggregated_price``. When ``None`` ``tvl_usd``
            is ``Decimal("0")`` (the same documented behaviour as the legacy
            reader — TVL is informational and not required for the reserve
            snapshot).
    """

    def __init__(
        self,
        rpc_call: Any,
        token_resolver: Any | None = None,
        price_oracle: Any | None = None,
    ) -> None:
        self._rpc_call = rpc_call
        self._token_resolver = token_resolver
        self._price_oracle = price_oracle

    async def get_pool_reserves(self, pool_address: str, chain: str) -> PoolReserves:
        """Read a pool's reserves and state through the gateway.

        Args:
            pool_address: Pool contract address.
            chain: Chain identifier (already lowercased by the snapshot).

        Returns:
            PoolReserves with full V3 pool state.

        Raises:
            DataUnavailableError: If a required chain read fails or the gateway
                proxy returns an empty / malformed response.
            DataSourceError: If reserve assembly fails for any other reason.
        """
        chain_lower = chain.lower()
        # The gateway eth_call proxy is synchronous; run the blocking reads in a
        # worker thread so this coroutine integrates with the snapshot's
        # ``_run_async_bridged`` bridge without blocking the event loop.
        return await asyncio.to_thread(self._read_pool_reserves_sync, pool_address, chain_lower)

    def _read_pool_reserves_sync(self, pool_address: str, chain: str) -> PoolReserves:
        try:
            sqrt_price_x96, tick = decode_slot0(self._rpc_call(chain, pool_address, SLOT0_SELECTOR))
            liquidity = decode_uint(self._rpc_call(chain, pool_address, LIQUIDITY_SELECTOR))
            token0_addr = decode_address(self._rpc_call(chain, pool_address, TOKEN0_SELECTOR))
            token1_addr = decode_address(self._rpc_call(chain, pool_address, TOKEN1_SELECTOR))
            fee_tier = decode_uint(self._rpc_call(chain, pool_address, FEE_SELECTOR))

            token0 = self._resolve_chain_token(token0_addr, chain)
            token1 = self._resolve_chain_token(token1_addr, chain)

            reserve0_raw = self._read_balance_of(token0_addr, pool_address, chain)
            reserve1_raw = self._read_balance_of(token1_addr, pool_address, chain)
            reserve0 = Decimal(reserve0_raw) / Decimal(10**token0.decimals)
            reserve1 = Decimal(reserve1_raw) / Decimal(10**token1.decimals)
        except DataUnavailableError:
            # Already the typed transient failure (short / malformed eth_call
            # response from the pure decoders, an out-of-range decimals read, or
            # a disconnected gateway surfaced by the rpc_call closure).
            raise
        except Exception as e:  # noqa: BLE001
            # A failed chain read / gateway proxy error is a *transient*
            # data-unavailable condition per this reader's contract (see the
            # ``Raises:`` docstring), not an assembly bug. Surface it as
            # DataUnavailableError so the runner classifies it DATA_UNAVAILABLE
            # (HOLD-inference) instead of a hard DataSourceError.
            raise DataUnavailableError(
                data_type="pool_reserves",
                instrument=pool_address,
                reason=f"chain read failed on '{chain}': {e}",
            ) from e

        # TVL is informational and deliberately OUTSIDE the data-unavailable
        # contract: _calculate_tvl_usd swallows its own oracle failures and falls
        # back to Decimal("0"), so it never blocks a valid reserve snapshot.
        tvl_usd = self._calculate_tvl_usd(reserve0, reserve1, token0.symbol, token1.symbol, chain)

        try:
            return PoolReserves(
                pool_address=pool_address,
                dex="uniswap_v3",
                token0=token0,
                token1=token1,
                reserve0=reserve0,
                reserve1=reserve1,
                fee_tier=int(fee_tier),
                sqrt_price_x96=int(sqrt_price_x96),
                tick=int(tick),
                liquidity=int(liquidity),
                tvl_usd=tvl_usd,
                last_updated=datetime.now(UTC),
            )
        except Exception as e:  # noqa: BLE001
            raise DataSourceError(f"Failed to assemble pool reserves for '{pool_address}' on '{chain}': {e}") from e

    def _read_balance_of(self, token_address: str, holder_address: str, chain: str) -> int:
        calldata = _BALANCE_OF_SELECTOR + holder_address.lower().removeprefix("0x").zfill(64)
        return decode_uint(self._rpc_call(chain, token_address, calldata))

    def _resolve_chain_token(self, token_address: str, chain: str) -> ChainToken:
        from ..tokens.models import ChainToken, Token

        symbol = _UNKNOWN_SYMBOL
        name = "Unknown Token"
        decimals: int | None = None

        if self._token_resolver is not None:
            try:
                resolved = self._token_resolver.resolve(token_address, chain, log_errors=False)
                symbol = getattr(resolved, "symbol", None) or symbol
                name = getattr(resolved, "name", None) or symbol
                resolved_decimals = getattr(resolved, "decimals", None)
                # Validate the registry value against the same bound as the
                # on-chain path; a malformed record falls through to the on-chain
                # read rather than mis-scaling reserves.
                if resolved_decimals is not None and _is_valid_decimals(resolved_decimals):
                    decimals = int(resolved_decimals)
            except Exception:  # noqa: BLE001 — fall back to on-chain decimals.
                decimals = None

        if decimals is None:
            # Empty != Zero: never assume 18. A missing/invalid registry decimal
            # would mis-scale the reserves by orders of magnitude, so read it
            # on-chain and let a genuine failure surface as DataUnavailableError.
            decimals = self._read_decimals_on_chain(token_address, chain)

        return ChainToken(
            token=Token(symbol=symbol, name=name, decimals=int(decimals), addresses={chain: token_address}),
            chain=chain,
            address=token_address,
            decimals=int(decimals),
        )

    def _read_decimals_on_chain(self, token_address: str, chain: str) -> int:
        try:
            decimals = decode_uint(self._rpc_call(chain, token_address, _DECIMALS_SELECTOR))
        except DataUnavailableError:
            raise
        except Exception as e:  # noqa: BLE001
            raise DataUnavailableError(
                data_type="pool_reserves",
                instrument=token_address,
                reason=f"Cannot determine decimals for token {token_address} on {chain}: {e}",
            ) from e
        if not _is_valid_decimals(decimals):
            # decode_uint accepts any 32-byte word: a malformed / malicious
            # decimals() would otherwise feed 10**decimals (huge-int allocation /
            # DoS) into reserve scaling. Fail loud as a typed data-unavailable.
            raise DataUnavailableError(
                data_type="pool_reserves",
                instrument=token_address,
                reason=(
                    f"decimals() returned {decimals} (outside 0..{_MAX_REASONABLE_DECIMALS}) "
                    f"for token {token_address} on {chain}"
                ),
            )
        return decimals

    def _calculate_tvl_usd(
        self,
        reserve0: Decimal,
        reserve1: Decimal,
        token0_symbol: str,
        token1_symbol: str,
        chain: str,
    ) -> Decimal:
        if self._price_oracle is None:
            return Decimal("0")
        if _UNKNOWN_SYMBOL in (token0_symbol, token1_symbol):
            # A registry miss leaves the symbol unresolved; pricing "UNKNOWN"
            # would fail anyway. TVL is best-effort informational — return 0 with
            # a signal rather than emitting a misleading partial value.
            _LOG.debug(
                "pool_reserves tvl_usd best-effort 0: unresolved token symbol(s) on %s (%s / %s)",
                chain,
                token0_symbol,
                token1_symbol,
            )
            return Decimal("0")
        try:
            price0 = self._get_price_sync(token0_symbol, chain)
            price1 = self._get_price_sync(token1_symbol, chain)
            if price0 is None or price1 is None:
                _LOG.debug("pool_reserves tvl_usd best-effort 0: price miss on %s", chain)
                return Decimal("0")
            return reserve0 * price0 + reserve1 * price1
        except Exception:  # noqa: BLE001 — TVL is informational; never block the snapshot.
            _LOG.debug("pool_reserves tvl_usd best-effort 0: oracle error on %s", chain, exc_info=True)
            return Decimal("0")

    def _get_price_sync(self, token_symbol: str, chain: str) -> Decimal | None:
        # Price exclusively via the documented PriceOracle Protocol method,
        # async ``get_aggregated_price(token, "USD", *, chain=...)`` — the
        # live-wired GatewayPriceOracle implements it. We deliberately do NOT
        # fall back to a sync ``get_price``: there is no single sync get_price
        # signature in the framework (data_provider, dashboard api_client and
        # prediction_provider all differ), so guessing one would silently
        # mis-price. An oracle without get_aggregated_price yields None -> TVL 0
        # (best-effort informational). A ``price`` of None is also None -> 0,
        # never Decimal("None").
        get_aggregated_price = getattr(self._price_oracle, "get_aggregated_price", None)
        if not callable(get_aggregated_price):
            return None
        if _supports_chain_kwarg(get_aggregated_price):
            result = asyncio.run(get_aggregated_price(token_symbol, "USD", chain=chain))
        else:
            result = asyncio.run(get_aggregated_price(token_symbol, "USD"))
        price = getattr(result, "price", None)
        return None if price is None else Decimal(str(price))


def _supports_chain_kwarg(get_aggregated_price: Any) -> bool:
    try:
        parameters = inspect.signature(get_aggregated_price).parameters.values()
    except (TypeError, ValueError):
        return True

    for parameter in parameters:
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            return True
        if parameter.name == "chain":
            return parameter.kind != inspect.Parameter.POSITIONAL_ONLY

    return False


__all__ = ["GatewayPoolReserveReader"]
