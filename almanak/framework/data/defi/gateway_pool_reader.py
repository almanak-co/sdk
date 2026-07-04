"""Gateway-backed DEX pool reserve reader (VIB-4845, T3-C).

``MarketSnapshot.pool_reserves(...)`` returns a :class:`PoolReserves`
snapshot of a DEX pool — token0/token1 metadata, human-readable reserves,
fee tier, and (for concentrated-liquidity pools) sqrtPriceX96, tick, and
in-range liquidity. The pool's AMM shape is auto-detected per address:

- **Uniswap-V3-shaped** (Uniswap V3, Slipstream, Pancake/Sushi V3): read via
  ``slot0`` / ``liquidity`` / ``fee`` / ``balanceOf``; ``dex="uniswap_v3"``.
- **Solidly-shaped** (Aerodrome / Velodrome classic pools): no ``slot0()`` —
  read via ``getReserves`` / ``stable``, fee via ``factory().getFee(pool,
  stable)``; ``dex="solidly_v2"``.
- **Plain V2 pairs** (``getReserves`` plus the ``price0CumulativeLast``
  oracle getter, no ``stable()``): ``dex="uniswap_v2"``. The V2 swap fee is
  not on-chain readable and fork fees differ (Uniswap 0.30%, PancakeSwap
  0.25%, ...), so ``fee_tier`` is ``None`` — unmeasured, never guessed.

The legacy :class:`almanak.framework.data.defi.pools.UniswapV3PoolReader`
that satisfies the same shape opens its own direct ``AsyncWeb3`` HTTP-provider
connections — a gateway-boundary violation that can only run in tests or
boundary-exempt connector internals, never in the strategy container.

This module provides the boundary-compliant equivalent: all chain reads go
through the gateway ``eth_call`` proxy (the ``rpc_call`` closure the builder
wires from ``gateway_client.eth_call``), reusing the pure decode helpers that
already back the gateway-backed price reader
(:mod:`almanak.framework.data.pools.reader`). No new gateway service is
required — slot0 / liquidity / fee / balanceOf / getReserves / stable /
price0CumulativeLast / factory / getFee / token0 / token1 / decimals are all
plain ``eth_call`` reads.

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
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.solidly_pool_abi import (
    SOLIDLY_FACTORY_SELECTOR,
    SOLIDLY_GET_RESERVES_SELECTOR,
    SOLIDLY_STABLE_SELECTOR,
    V2_PRICE0_CUMULATIVE_LAST_SELECTOR,
    encode_solidly_get_fee,
)
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

from .pools import DexType, PoolReserves

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

# Solidly ``PoolFactory.getFee`` returns basis points (30 = 0.30%);
# ``PoolReserves.fee_tier`` uses Uniswap units (hundredths of a basis point,
# 3000 = 0.30%).
_SOLIDLY_FEE_BPS_TO_FEE_TIER = 100

# Internal ABI-shape tags for the per-pool shape cache. Deliberately NOT
# protocol names: the cache dispatches on how to READ a pool (which ABI it
# answers), while ``dex`` only labels what to stamp — protocol-conditional
# branching belongs in connector manifests, not framework code (the
# protocol-literal ratchet enforces this).
_SHAPE_SLOT0 = "slot0"
_SHAPE_RESERVES = "get_reserves"

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
    """Reads DEX pool reserves through the gateway eth_call proxy.

    The reader is intentionally protocol-shaped like the legacy
    ``UniswapV3PoolReader`` (async ``get_pool_reserves(pool_address, chain)``)
    so ``MarketSnapshot.pool_reserves`` can call it without change, but it never
    opens a socket itself — every read is an ``eth_call`` through the gateway.

    The pool's AMM shape is auto-detected (see module docstring): V3-shaped
    pools read via ``slot0``/``liquidity``/``fee``; Solidly-shaped pools
    (Aerodrome / Velodrome classic — no ``slot0()``) and plain V2 pairs read
    via ``getReserves``. The detected shape is cached per ``(chain, pool)`` so
    repeat reads pay no extra probe calls.

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
        # (chain, pool) -> (ABI shape tag, Solidly stable flag). A pool's ABI
        # shape is immutable, so a successful detection never needs to be
        # re-probed for the lifetime of this reader. Written ONLY after a fully
        # successful read backed by a definitive positive probe signal, so a
        # transient failure can never pin a wrong shape. ``stable`` is a real
        # bool for Solidly pools and None otherwise.
        self._pool_shape_cache: dict[tuple[str, str], tuple[str, bool | None]] = {}
        # Immutable per-pool facts (token0/token1 metadata, factory address)
        # cached on first successful resolution so repeat snapshot reads pay
        # only the mutable-state calls (reserves / fee / slot0 / balanceOf).
        self._pool_token_cache: dict[tuple[str, str], tuple[ChainToken, ChainToken]] = {}
        self._pool_factory_cache: dict[tuple[str, str], str] = {}

    async def get_pool_reserves(self, pool_address: str, chain: str) -> PoolReserves:
        """Read a pool's reserves and state through the gateway.

        Args:
            pool_address: Pool contract address.
            chain: Chain identifier (already lowercased by the snapshot).

        Returns:
            PoolReserves with the pool's state; fields are populated per the
            detected AMM shape (``dex`` is ``uniswap_v3``, ``solidly_v2`` or
            ``uniswap_v2``; ``fee_tier`` is None for V2 pairs — unmeasured).

        Raises:
            DataUnavailableError: If a required chain read fails, the gateway
                proxy returns an empty / malformed response, or the pool
                matches no supported AMM shape.
            DataSourceError: If reserve assembly fails for any other reason.
        """
        chain_lower = chain.lower()
        # The gateway eth_call proxy is synchronous; run the blocking reads in a
        # worker thread so this coroutine integrates with the snapshot's
        # ``_run_async_bridged`` bridge without blocking the event loop.
        return await asyncio.to_thread(self._read_pool_reserves_sync, pool_address, chain_lower)

    def _read_pool_reserves_sync(self, pool_address: str, chain: str) -> PoolReserves:
        cached = self._pool_shape_cache.get((chain, pool_address.lower()))
        if cached is not None:
            shape, stable = cached
            if shape == _SHAPE_SLOT0:
                return self._read_v3_pool_sync(pool_address, chain)
            return self._read_reserves_pool_sync(pool_address, chain, classified=True, stable=stable)

        # Shape unknown: probe slot0() first (V3-shaped pools dominate). A
        # Solidly / V2 pool has no slot0(), so the eth_call reverts — surfaced
        # either as empty bytes or as an exception, depending on how the
        # gateway proxies the revert. Either way, fall through to the
        # getReserves-shaped read, which raises the definitive
        # DataUnavailableError when the pool matches no supported shape.
        try:
            slot0_raw = self._rpc_call(chain, pool_address, SLOT0_SELECTOR)
            probe_note = f"slot0() returned {len(slot0_raw)} bytes"
        except Exception as e:  # noqa: BLE001 — revert or transient; the reserves probe decides
            slot0_raw = b""
            probe_note = f"slot0() failed: {e}"
        if len(slot0_raw) >= 64:
            return self._read_v3_pool_sync(pool_address, chain, slot0_raw=slot0_raw)
        return self._read_reserves_pool_sync(pool_address, chain, probe_note=probe_note)

    def _read_v3_pool_sync(self, pool_address: str, chain: str, slot0_raw: bytes | None = None) -> PoolReserves:
        with self._chain_read_guard(pool_address, chain):
            raw = slot0_raw if slot0_raw is not None else self._rpc_call(chain, pool_address, SLOT0_SELECTOR)
            sqrt_price_x96, tick = decode_slot0(raw)
            liquidity = decode_uint(self._rpc_call(chain, pool_address, LIQUIDITY_SELECTOR))
            fee_tier = decode_uint(self._rpc_call(chain, pool_address, FEE_SELECTOR))
            token0, token1 = self._resolve_pool_tokens(pool_address, chain)
            reserve0_raw = self._read_balance_of(token0.address, pool_address, chain)
            reserve1_raw = self._read_balance_of(token1.address, pool_address, chain)
            reserve0 = Decimal(reserve0_raw) / Decimal(10**token0.decimals)
            reserve1 = Decimal(reserve1_raw) / Decimal(10**token1.decimals)

        # Cache only after the full read succeeded (uniform with the
        # getReserves path): a transient mid-sequence failure never pins a
        # shape, even a plausible-looking one.
        self._pool_shape_cache[(chain, pool_address.lower())] = (_SHAPE_SLOT0, None)

        # TVL is informational and deliberately OUTSIDE the data-unavailable
        # contract: _calculate_tvl_usd swallows its own oracle failures and falls
        # back to Decimal("0"), so it never blocks a valid reserve snapshot.
        tvl_usd = self._calculate_tvl_usd(reserve0, reserve1, token0.symbol, token1.symbol, chain)

        with self._assembly_guard(pool_address, chain):
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

    def _read_reserves_pool_sync(
        self,
        pool_address: str,
        chain: str,
        *,
        classified: bool = False,
        stable: bool | None = None,
        probe_note: str | None = None,
    ) -> PoolReserves:
        """Read a getReserves-shaped pool (Solidly classic or plain V2 pair).

        ``classified=True`` (cache hit) skips re-classification and trusts the
        passed ``stable``; on first read the pool is classified by
        ``_classify_reserves_pool`` (definitive positive signals only).
        ``probe_note`` carries the slot0() probe outcome so a pool matching no
        shape reports both misses.
        """
        with self._chain_read_guard(pool_address, chain):
            reserves_raw = self._rpc_call(chain, pool_address, SOLIDLY_GET_RESERVES_SELECTOR)
            if len(reserves_raw) < 96:
                if classified:
                    # Shape already known (cache hit) — this is a transient
                    # short response, not an unsupported pool.
                    reason = f"getReserves() response too short: {len(reserves_raw)} bytes (need >= 96)"
                else:
                    # slot0() already missed (that's how we got here), so a
                    # short getReserves() means no supported AMM shape answers.
                    reason = (
                        f"pool answers neither slot0() (Uniswap-V3-shaped) nor "
                        f"getReserves() (Solidly/V2-shaped) on '{chain}'"
                    )
                    if probe_note:
                        reason = f"{reason}; {probe_note}"
                raise DataUnavailableError(
                    data_type="pool_reserves",
                    instrument=pool_address,
                    reason=reason,
                )
            reserve0_raw = decode_uint(reserves_raw[0:32])
            reserve1_raw = decode_uint(reserves_raw[32:64])

            if not classified:
                stable = self._classify_reserves_pool(pool_address, chain)
            # ``stable is not None`` IS the Solidly signal (a real bool from the
            # pool's own stable() getter); None means the V2-family positive
            # signal answered instead. PoolReserves.__post_init__ enforces the
            # same invariant.
            dex: DexType = "solidly_v2" if stable is not None else "uniswap_v2"

            # Solidly fees live on the factory and are read per call (factories
            # can change them). Plain V2 pairs expose no fee getter and fork
            # fees differ (Uniswap 0.30%, PancakeSwap 0.25%, ...), so the fee
            # is unmeasured there — None per Empty != Zero, never a guess.
            fee_tier = self._read_solidly_fee_tier(pool_address, chain, stable=stable) if stable is not None else None

            token0, token1 = self._resolve_pool_tokens(pool_address, chain)
            reserve0 = Decimal(reserve0_raw) / Decimal(10**token0.decimals)
            reserve1 = Decimal(reserve1_raw) / Decimal(10**token1.decimals)

        # Cache only after the full read succeeded: a transient mid-sequence
        # failure never pins a wrong shape for the reader's lifetime.
        self._pool_shape_cache[(chain, pool_address.lower())] = (_SHAPE_RESERVES, stable)

        tvl_usd = self._calculate_tvl_usd(reserve0, reserve1, token0.symbol, token1.symbol, chain)

        with self._assembly_guard(pool_address, chain):
            return PoolReserves(
                pool_address=pool_address,
                dex=dex,
                token0=token0,
                token1=token1,
                reserve0=reserve0,
                reserve1=reserve1,
                fee_tier=fee_tier,
                stable=stable,
                tvl_usd=tvl_usd,
                last_updated=datetime.now(UTC),
            )

    def _classify_reserves_pool(self, pool_address: str, chain: str) -> bool | None:
        """Classify a getReserves-shaped pool from definitive positive signals.

        Returns the Solidly ``stable`` flag (a real bool) when the pool proves
        Solidly, or None when it proves V2-family — downstream code dispatches
        on ``stable is not None``, never on a protocol-name comparison.

        The gateway eth_call proxy collapses execution reverts AND transient
        transport failures into the same empty response, so "stable() returned
        nothing" alone must never downgrade a pool to V2 — one flaky call would
        pin the wrong fee and price curve in the shape cache (Empty != Zero
        applied to classification). Each family therefore requires its own
        positive signal:

        - ``stable() -> bool`` word: Solidly (Aerodrome / Velodrome classic).
        - ``price0CumulativeLast()`` word: the canonical V2 oracle getter —
          answered by Uniswap-V2-family pairs, absent on Solidly pools (they
          expose ``reserve0CumulativeLast`` instead).

        Neither answering is ambiguous (transient failure, or an unsupported
        constant-product fork) — raise the typed transient error, leave the
        shape cache untouched, and let the next read re-probe.
        """
        stable_raw = self._probe(pool_address, chain, SOLIDLY_STABLE_SELECTOR)
        if len(stable_raw) >= 32:
            return bool(decode_uint(stable_raw))
        cumulative_raw = self._probe(pool_address, chain, V2_PRICE0_CUMULATIVE_LAST_SELECTOR)
        if len(cumulative_raw) >= 32:
            return None
        raise DataUnavailableError(
            data_type="pool_reserves",
            instrument=pool_address,
            reason=(
                f"cannot classify getReserves-shaped pool on '{chain}': neither stable() (Solidly) "
                f"nor price0CumulativeLast() (V2) answered — transient failure or unsupported AMM fork"
            ),
        )

    def _probe(self, pool_address: str, chain: str, selector: str) -> bytes:
        """Optional-function probe: empty bytes when the proxy surfaces the revert as an exception."""
        try:
            return self._rpc_call(chain, pool_address, selector)
        except Exception:  # noqa: BLE001 — probe misses must not abort classification
            return b""

    def _read_solidly_fee_tier(self, pool_address: str, chain: str, *, stable: bool) -> int:
        """Read a Solidly pool's swap fee from its factory.

        Solidly pools carry no fee getter themselves — the factory owns it
        (``getFee(pool, stable)``, basis points). The fee is never guessed:
        failures raise typed errors carrying the pool/factory context, so a
        fork whose factory lacks ``getFee(address,bool)`` surfaces with an
        actionable message instead of a context-free decode error.
        """
        key = (chain, pool_address.lower())
        factory_addr = self._pool_factory_cache.get(key)
        if factory_addr is None:
            # The factory address is immutable; cache it. The fee itself is
            # NOT cached — factories can change it, so it is read per call.
            factory_raw = self._rpc_call(chain, pool_address, SOLIDLY_FACTORY_SELECTOR)
            if len(factory_raw) < 32:
                raise DataUnavailableError(
                    data_type="pool_reserves",
                    instrument=pool_address,
                    reason=f"factory() returned no data for Solidly pool {pool_address} on '{chain}'",
                )
            factory_addr = decode_address(factory_raw)
            self._pool_factory_cache[key] = factory_addr
        fee_raw = self._rpc_call(chain, factory_addr, encode_solidly_get_fee(pool_address, stable))
        if len(fee_raw) < 32:
            raise DataUnavailableError(
                data_type="pool_reserves",
                instrument=pool_address,
                reason=(
                    f"getFee({pool_address}, stable={stable}) returned no data from factory "
                    f"{factory_addr} on '{chain}' — transient failure, or a Solidly fork "
                    f"with a different fee interface"
                ),
            )
        return decode_uint(fee_raw) * _SOLIDLY_FEE_BPS_TO_FEE_TIER

    @contextmanager
    def _chain_read_guard(self, pool_address: str, chain: str) -> Iterator[None]:
        """Single source of the reader's transient-error contract.

        DataUnavailableError passes through untouched (already the typed
        transient failure — a short / malformed eth_call response from the
        pure decoders, an out-of-range decimals read, or a disconnected
        gateway surfaced by the rpc_call closure). Any other exception raised
        during a chain-read block is a *transient* data-unavailable condition
        per this reader's contract, not an assembly bug, so the runner
        classifies it DATA_UNAVAILABLE (HOLD-inference) instead of a hard
        DataSourceError.
        """
        try:
            yield
        except DataUnavailableError:
            raise
        except Exception as e:  # noqa: BLE001
            raise DataUnavailableError(
                data_type="pool_reserves",
                instrument=pool_address,
                reason=f"chain read failed on '{chain}': {e}",
            ) from e

    @contextmanager
    def _assembly_guard(self, pool_address: str, chain: str) -> Iterator[None]:
        """Dataclass-assembly failures are hard errors, not transient ones."""
        try:
            yield
        except Exception as e:  # noqa: BLE001
            raise DataSourceError(f"Failed to assemble pool reserves for '{pool_address}' on '{chain}': {e}") from e

    def _resolve_pool_tokens(self, pool_address: str, chain: str) -> tuple[ChainToken, ChainToken]:
        """Decode token0()/token1() and resolve both to ChainTokens.

        token0/token1 (and their decimals) are immutable pool facts, so a
        successful resolution is cached for the reader's lifetime.
        """
        key = (chain, pool_address.lower())
        cached = self._pool_token_cache.get(key)
        if cached is not None:
            return cached
        token0_addr = decode_address(self._rpc_call(chain, pool_address, TOKEN0_SELECTOR))
        token1_addr = decode_address(self._rpc_call(chain, pool_address, TOKEN1_SELECTOR))
        pair = (self._resolve_chain_token(token0_addr, chain), self._resolve_chain_token(token1_addr, chain))
        self._pool_token_cache[key] = pair
        return pair

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
