"""Live pool reader for Aerodrome classic (Solidly vAMM/sAMM fungible pools).

Fills the classic half of the Aerodrome pool-data spec (its slot was declared
"not wired" since the two pool families were separated). Reuses the V3 base
reader's cache/envelope/resolution plumbing; only the on-chain shape differs:
``getReserves()`` instead of ``slot0()``, the stable/volatile flag as the
getPool key, and the swap fee read from the factory (``getFee(pool, stable)``,
denominator 1e4 → v3 1e-6 units is a ×100 conversion; verified on-chain for
the Base AERO/USDC volatile pool: getFee=30 → 0.30%).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal

from almanak.core.finality import DataFinality, parse_data_finality
from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.pools.reader import (
    PoolPrice,
    UniswapV3PoolPriceReader,
    decode_address,
    decode_uint,
)

GET_RESERVES_SELECTOR = "0x0902f1ac"
STABLE_SELECTOR = "0x22be3de1"
GET_FEE_SELECTOR = "0xcc56b2c5"
TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"
DECIMALS_SELECTOR = "0x313ce567"

_FEE_TO_V3_UNITS = 100  # Solidly fee denominator 1e4 → v3 1e-6 fee units


class SolidlyPoolReader(UniswapV3PoolPriceReader):
    """Live price reads for fungible Solidly pools (vAMM ratio / sAMM marginal)."""

    def __init__(self, *args, **kwargs) -> None:
        if kwargs.get("spec") is None:
            raise ValueError(
                "SolidlyPoolReader is kind-dispatched and must be constructed "
                "with its connector PoolReaderSpec (use PoolReaderRegistry.get_reader)."
            )
        super().__init__(*args, **kwargs)

    def read_pool_price(
        self,
        pool_address: str,
        chain: str,
        block_number: int | None = None,
        finality: DataFinality = DataFinality.LATEST,
    ) -> DataEnvelope[PoolPrice]:
        finality = parse_data_finality(finality)
        chain_lower = chain.lower()
        cache_key = (pool_address.lower(), chain_lower)
        cached = self._cache_lookup(cache_key)
        if cached is not None:
            return cached

        start_time = time.monotonic()
        try:
            reserves = self._rpc_call(chain_lower, pool_address, GET_RESERVES_SELECTOR)
            reserve0 = int.from_bytes(reserves[0:32], "big")
            reserve1 = int.from_bytes(reserves[32:64], "big")
            token0 = decode_address(self._rpc_call(chain_lower, pool_address, TOKEN0_SELECTOR))
            token1 = decode_address(self._rpc_call(chain_lower, pool_address, TOKEN1_SELECTOR))
            token0_decimals = self._solidly_decimals(token0, chain_lower)
            token1_decimals = self._solidly_decimals(token1, chain_lower)
            stable = bool(decode_uint(self._rpc_call(chain_lower, pool_address, STABLE_SELECTOR)))
            fee_tier = self._read_fee_tier(chain_lower, pool_address, stable)
            if reserve0 == 0 or reserve1 == 0:
                raise ValueError("empty pool: one or more reserves are zero")
            x = Decimal(reserve0) / Decimal(10) ** token0_decimals
            y = Decimal(reserve1) / Decimal(10) ** token1_decimals
            if stable:
                # sAMM invariant x³y+xy³: marginal price dy/dx, not the reserve
                # ratio (which diverges from the executable price off-peg).
                price = (3 * x * x * y + y**3) / (x**3 + 3 * x * y * y)
            else:
                price = y / x
        except DataUnavailableError:
            raise
        except Exception as e:
            raise DataUnavailableError(
                data_type="pool_price",
                instrument=pool_address,
                reason=f"Solidly pool read failed for {pool_address} on {chain_lower}: {e}",
            ) from e

        latency_ms = int((time.monotonic() - start_time) * 1000)
        observed_at = datetime.now(UTC)
        pool_price = PoolPrice(
            price=price,
            tick=None,
            liquidity=reserve0,  # depth proxy for non-tick AMMs: raw token0 reserve
            fee_tier=fee_tier,
            block_number=block_number or 0,
            timestamp=observed_at,
            pool_address=pool_address,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
        )
        envelope = DataEnvelope(
            value=pool_price,
            meta=DataMeta(
                source=self._source_name,
                observed_at=observed_at,
                block_number=block_number if block_number else None,
                finality=finality,
                staleness_ms=0,
                latency_ms=latency_ms,
                confidence=1.0,
                cache_hit=False,
            ),
            classification=DataClassification.EXECUTION_GRADE,
        )
        self._cache[cache_key] = (time.monotonic(), envelope)
        return envelope

    def resolve_pool_address(
        self,
        token_a: str,
        token_b: str,
        chain: str,
        fee_tier: int = 3000,
    ) -> str | None:
        """Resolve a classic pool; ``fee_tier`` carries the stable flag.

        ``0``/``1`` pin volatile/stable exactly. Any other value (generic v3
        fee-tier sweeps from LWAP / pair consumers, which would otherwise be
        ABI-encoded into the factory's ``bool`` argument and never resolve)
        resolves BOTH flavours and returns the deeper — a total function per
        pair, so tier sweeps multi-key one pool and the aggregator's address
        dedupe collapses them (same contract as the Curve reader).
        """
        if fee_tier in (0, 1):
            return super().resolve_pool_address(token_a, token_b, chain, fee_tier=fee_tier)
        best, best_reserve = None, -1
        for flag in (0, 1):
            pool = super().resolve_pool_address(token_a, token_b, chain, fee_tier=flag)
            if pool is None:
                continue
            try:
                reserves = self._rpc_call(chain.lower(), pool, GET_RESERVES_SELECTOR)
                reserve0 = int.from_bytes(reserves[0:32], "big")
            except Exception:  # noqa: BLE001 — unreadable candidate ranks last, not fatal
                reserve0 = 0
            if reserve0 > best_reserve:
                best, best_reserve = pool, reserve0
        return best

    def _solidly_decimals(self, token_address: str, chain: str) -> int:
        if self._token_resolver is not None:
            try:
                resolved = self._token_resolver.resolve(token_address, chain)
                if resolved is not None and getattr(resolved, "decimals", None) is not None:
                    return int(resolved.decimals)
            except Exception:  # noqa: BLE001 — fall back to the on-chain read
                pass
        return decode_uint(self._rpc_call(chain, token_address, DECIMALS_SELECTOR))

    def _read_fee_tier(self, chain: str, pool_address: str, stable: bool) -> int:
        factory = self._factory_addresses.get(chain)
        if not factory:
            return 0
        calldata = (
            GET_FEE_SELECTOR + pool_address.lower().removeprefix("0x").zfill(64) + ("1" if stable else "0").zfill(64)
        )
        try:
            return decode_uint(self._rpc_call(chain, factory, calldata)) * _FEE_TO_V3_UNITS
        except Exception:  # noqa: BLE001 — fee is informational; identity fields stay exact
            return 0
