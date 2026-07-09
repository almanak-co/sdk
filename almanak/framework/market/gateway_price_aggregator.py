"""Gateway-backed price aggregator for the live / hosted MarketSnapshot.

VIB-4924 / ALM-2770 — ``MarketSnapshot.twap()`` / ``lwap()`` were unwired in
the hosted runner because ``MarketSnapshotBuilder.for_strategy_runner`` never
injected a ``price_aggregator``. This module supplies the aggregator the live
builder injects.

Design (see ``docs/internal/VIB-4845-twap-lwap-wiring-ALM-2770-2026-06-01.md``):

- **twap()** routes through the deployed gateway ``RateHistoryService.GetDexTwap``
  service (VIB-4859). The gateway owns the per-connector ``observe()`` semantics
  and returns a human-readable price with honest ``source="on_chain"`` provenance
  — so we do NOT re-read ``observe()`` framework-side (which would both duplicate
  gateway logic and stamp the false ``alchemy_rpc`` provenance of the base
  ``PriceAggregator.twap``).

- **lwap()** routes through the gateway ``RateHistoryService.GetDexLwap``
  service (VIB-4948, L3). Pool *resolution* stays framework-side (reuse the
  registry — known-pools table + ``factory.getPool``), then the resolved pool
  addresses go in a single ``GetDexLwap`` call so the gateway does the
  N×(slot0+liquidity+decimals) reads server-side, concurrently, and returns the
  liquidity-weighted price with ``source="gateway_rpc"``. This supersedes the
  inherited ``eth_call``-orchestrated ``PriceAggregator.lwap`` (L2) on the live
  path; the inherited implementation remains for the base class / backtest math.

twap and lwap therefore carry *different* provenance — ``on_chain`` (the gateway
DEX-TWAP oracle) vs ``gateway_rpc`` (liquidity-weighted spot read server-side).
That is honest: they are genuinely different mechanisms, not the F2 lie.

Both paths remain gateway-boundary compliant: every byte flows through the
gateway gRPC channel; the strategy container makes no direct egress.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)
from almanak.framework.data.pools.aggregation import (
    AggregatedPrice,
    PoolContribution,
    PriceAggregator,
)
from almanak.framework.data.pools.reader import PoolReaderRegistry
from almanak.framework.market.errors import PoolPriceUnavailableError


class GatewayMarketPriceAggregator(PriceAggregator):
    """``twap()`` via gateway ``GetDexTwap``; ``lwap()`` via gateway ``GetDexLwap``.

    Args:
        gateway_client: Connected ``GatewayClient`` exposing the ``rate_history``
            stub (``GetDexTwap`` / ``GetDexLwap``).
        pool_registry: Shared ``PoolReaderRegistry`` (constructed with
            ``source_name="gateway_rpc"``) used for pair→pool resolution (twap)
            and candidate-pool resolution (lwap).
        rpc_call: Gateway ``eth_call`` closure shared with ``pool_registry``.
    """

    # The gateway returns a human-readable TWAP price, so MarketSnapshot.twap
    # must NOT spend extra eth_calls resolving token decimals (VIB-4924 §6.3).
    requires_decimals: bool = False

    def __init__(
        self,
        gateway_client: Any,
        pool_registry: PoolReaderRegistry,
        rpc_call: Any,
    ) -> None:
        super().__init__(
            pool_registry=pool_registry,
            rpc_call=rpc_call,
            source_name="gateway_rpc",
        )
        self._gateway_client = gateway_client

    def twap(
        self,
        pool_address: str,
        chain: str,
        window_seconds: int = 300,
        token0_decimals: int | None = None,  # noqa: ARG002 — gateway returns human-readable price
        token1_decimals: int | None = None,  # noqa: ARG002 — gateway returns human-readable price
        protocol: str = "uniswap_v3",
    ) -> DataEnvelope[AggregatedPrice]:
        """Return the pool TWAP from the gateway ``GetDexTwap`` service.

        Maps ``window_seconds`` → ``secs_ago_start`` (with ``secs_ago_end=0``)
        and ``protocol`` → ``dex``. ``token*_decimals`` are accepted and ignored
        (the gateway price is already human-readable) so the
        ``MarketSnapshot.twap`` call site is unchanged.

        Errors degrade cleanly: an RPC failure, an unconnected ``rate_history``
        stub (``RuntimeError``), or a gateway ``success=false`` (which already
        enumerates unsupported dex/chain, so we do NOT keep a second
        framework-side allowlist — VIB-4924 H2) all surface as
        ``PoolPriceUnavailableError``.
        """
        from almanak.gateway.proto import gateway_pb2

        start_time = datetime.now(UTC)
        try:
            resp = self._gateway_client.rate_history.GetDexTwap(
                gateway_pb2.GetDexTwapRequest(
                    dex=protocol,
                    chain=chain,
                    pool_address=pool_address,
                    secs_ago_start=window_seconds,
                    secs_ago_end=0,
                )
            )
        except Exception as e:  # noqa: BLE001 — RpcError / unconnected-stub RuntimeError
            raise PoolPriceUnavailableError(
                pool_address,
                f"GetDexTwap RPC failed on {chain}/{protocol}: {e}",
            ) from e

        if not resp.success:
            raise PoolPriceUnavailableError(
                pool_address,
                f"TWAP unavailable on {chain}/{protocol}: {resp.error or 'success=false'}",
            )

        price = Decimal(resp.point.price)
        contribution = PoolContribution(
            pool_address=pool_address,
            protocol=protocol,
            price=price,
            weight=1.0,
        )
        aggregated = AggregatedPrice(
            price=price,
            sources=[contribution],
            block_range=(0, 0),
            method="twap",
            window_seconds=window_seconds,
            pool_count=1,
        )
        meta = DataMeta(
            # Honest provenance: the servicer stamps "on_chain" on success
            # (rate_history_service.py) — never "alchemy_rpc" (VIB-4924 F2/M3).
            source=resp.source or "on_chain",
            observed_at=start_time,
            finality="latest",
            staleness_ms=0,
            latency_ms=int((datetime.now(UTC) - start_time).total_seconds() * 1000),
            confidence=1.0,
            cache_hit=False,
        )
        return DataEnvelope(
            value=aggregated,
            meta=meta,
            classification=DataClassification.EXECUTION_GRADE,
        )

    def lwap(
        self,
        token_a: str,
        token_b: str,
        chain: str,
        fee_tiers: list[int] | None = None,
        protocols: list[str] | None = None,
    ) -> DataEnvelope[AggregatedPrice]:
        """Liquidity-weighted spot price via the gateway ``GetDexLwap`` service (L3).

        Pool resolution stays framework-side (reuse the registry — known-pools
        table + ``factory.getPool``); the resolved pool addresses are sent in a
        single ``GetDexLwap`` call so the gateway does the N×(slot0+liquidity+
        decimals) reads server-side, concurrently, and returns the weighted
        price (VIB-4948). Provenance is ``source="gateway_rpc"``.

        Falls back through the same structured ``PoolPriceUnavailableError`` as
        ``twap()`` on RPC failure / unconnected stub / ``success=false``.
        """
        from almanak.gateway.proto import gateway_pb2

        chain_lower = chain.lower()
        pair = f"{token_a}/{token_b}"
        if fee_tiers is None:
            # Union of Uniswap-style fee tiers and Aerodrome Slipstream tick
            # spacings — pools are keyed by tick spacing on Slipstream and by
            # fee tier on Uniswap/forks. Discriminators with no pool resolve to
            # the zero address via factory.getPool and are skipped, so the
            # superset is safe: one sweep covers both pool-key models.
            fee_tiers = [1, 10, 50, 100, 200, 500, 2000, 3000, 10000]
        if protocols is None:
            protocols = self._registry.protocols_for_chain(chain_lower)
        if not protocols:
            raise PoolPriceUnavailableError(pair, f"No protocols registered for chain '{chain_lower}'")

        # Resolve candidate pools across protocols × fee tiers (known-pools +
        # factory.getPool over the gateway eth_call proxy). The gateway read is
        # protocol-agnostic ACROSS THE SLOT0 FAMILY (slot0/liquidity), so all
        # resolved pools go in one call under the uniswap_v3 read profile (the
        # registered LWAP-capable dex); unreadable pools are skipped
        # server-side. Non-slot0 reader kinds (e.g. Curve's get_dy shape) are
        # excluded up front — their pools cannot be read under this profile,
        # so shipping them would only add doomed server-side reads; the
        # framework PriceAggregator.lwap lane covers those protocols with
        # their own reader.
        pool_addresses: list[str] = []
        seen: set[str] = set()
        base_addr: str | None = None
        quote_addr: str | None = None
        for protocol in protocols:
            if self._registry.reader_kind(protocol) != "v3_slot0":
                continue
            try:
                reader = self._registry.get_reader(chain_lower, protocol)
            except ValueError:
                continue
            # Resolve the pair → token addresses once (any reader shares the
            # registry's token_resolver). Forwarded to GetDexLwap so the gateway
            # drops any candidate pool that is not exactly this pair — a stale
            # known-pools entry pointing at a different pair would otherwise
            # poison the liquidity-weighted aggregate (VIB-4924 B2 follow-on).
            if base_addr is None:
                base_addr = reader._resolve_to_address(token_a, chain_lower)
                quote_addr = reader._resolve_to_address(token_b, chain_lower)
            for fee_tier in fee_tiers:
                addr = reader.resolve_pool_address(token_a, token_b, chain_lower, fee_tier)
                if addr and addr.lower() not in seen:
                    seen.add(addr.lower())
                    pool_addresses.append(addr)

        if not pool_addresses:
            raise PoolPriceUnavailableError(pair, f"No pools resolved for {pair} on {chain_lower}")

        # The gateway LWAP read is protocol-agnostic — it reads slot0() +
        # liquidity() on the already-resolved pool addresses, and the uniswap_v3
        # provider reads Uniswap V3 AND all its V3-style forks (PancakeSwap V3,
        # SushiSwap V3, Aerodrome Slipstream) identically. `protocols` governs
        # pool *resolution* only, not the read. We therefore always dispatch
        # under the uniswap_v3 read profile rather than `protocols[0]`, which
        # would route to a dex with no registered LWAP provider whenever a
        # caller pins a non-uniswap protocol (e.g. protocols=["aerodrome_slipstream"])
        # and hard-fail with "unsupported dex (lwap)".
        dex = "uniswap_v3"
        start_time = datetime.now(UTC)
        try:
            resp = self._gateway_client.rate_history.GetDexLwap(
                gateway_pb2.GetDexLwapRequest(
                    dex=dex,
                    chain=chain_lower,
                    pool_addresses=pool_addresses,
                    base_token=base_addr or "",
                    quote_token=quote_addr or "",
                )
            )
        except Exception as e:  # noqa: BLE001 — RpcError / unconnected-stub RuntimeError
            raise PoolPriceUnavailableError(pair, f"GetDexLwap RPC failed on {chain_lower}: {e}") from e

        if not resp.success:
            raise PoolPriceUnavailableError(pair, f"LWAP unavailable on {chain_lower}: {resp.error or 'success=false'}")

        aggregated = AggregatedPrice(
            price=Decimal(resp.point.price),
            sources=[],
            block_range=(0, 0),
            method="lwap",
            pool_count=resp.point.pool_count,
        )
        meta = DataMeta(
            source=resp.source or "gateway_rpc",
            observed_at=start_time,
            finality="latest",
            staleness_ms=0,
            latency_ms=int((datetime.now(UTC) - start_time).total_seconds() * 1000),
            confidence=1.0,
            cache_hit=False,
        )
        return DataEnvelope(
            value=aggregated,
            meta=meta,
            classification=DataClassification.EXECUTION_GRADE,
        )


__all__ = ["GatewayMarketPriceAggregator"]
