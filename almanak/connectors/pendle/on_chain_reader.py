"""On-chain reader for Pendle market pricing.

Provides fallback pricing when the Pendle REST API is unavailable.

**PT-to-asset rate** (the canonical money-path read consumed by the gateway's
``GetPtPrice`` valuation) is read from the per-chain **PendlePYLpOracle** via the
2-arg ``getPtToAssetRate(market, duration)`` TWAP call. This is a deliberate move
off the legacy ``RouterStatic.getPtToAssetRate(market)`` spot read (VIB-5333):

- Pendle has **decommissioned** ``RouterStatic`` on Arbitrum — the historical
  address has no code, so every Arbitrum PT rate read failed and the PT was valued
  at $0. The PT oracle answers on both Ethereum and Arbitrum.
- The TWAP read is manipulation-resistant (a strict improvement over a single-block
  spot read for accounting valuation).

``getImpliedApy`` remains a ``RouterStatic``-only function and is retained on the
RouterStatic path purely for informational health display; it is NOT on the money
path and degrades gracefully on chains without a registered RouterStatic.

``readTokens`` / ``expiry`` are read from the **market** contract directly (no
RouterStatic dependency), so they are chain-portable.

Supports two modes:
- **Gateway mode** (preferred): Routes reads through the gateway's RpcService
  using raw eth_call, respecting the gateway-only architecture.
- **Direct mode** (legacy): Uses web3.py with a direct RPC URL. Only for
  local development or when no gateway is available.
"""

import json
import logging
import threading
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from .addresses import PENDLE as _PENDLE

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

# Chains for which the connector supports on-chain market reads. Scoped to the
# chains that have a registered PT oracle (the rate money path); broaden only with
# a verified pt_oracle entry in ``addresses.PENDLE`` plus tests.
_SUPPORTED_CHAINS: tuple[str, ...] = ("ethereum", "arbitrum")

# PendlePYLpOracle addresses per chain — canonical, manipulation-resistant TWAP
# rate source. Single source of truth: ``addresses.PENDLE[chain]["pt_oracle"]``.
PT_ORACLE_ADDRESSES: dict[str, str] = {
    chain: _PENDLE[chain]["pt_oracle"] for chain in _SUPPORTED_CHAINS if "pt_oracle" in _PENDLE[chain]
}

# RouterStatic addresses per chain — retained ONLY for ``getImpliedApy`` (an
# informational health read, NOT the money path). Pendle has decommissioned
# RouterStatic on Arbitrum, so it is absent there and ``get_implied_apy`` degrades
# gracefully. Single source of truth: ``addresses.PENDLE[chain]["router_static"]``.
ROUTER_STATIC_ADDRESSES: dict[str, str] = {
    chain: _PENDLE[chain]["router_static"] for chain in _SUPPORTED_CHAINS if "router_static" in _PENDLE[chain]
}

# 15-minute TWAP window for the PT oracle. MUST be > 0 — a duration of 0 reverts
# on-chain with a division-by-zero inside the oracle.
PT_ORACLE_TWAP_DURATION_SECONDS = 900

# Function selectors (keccak256 of canonical signatures, first 4 bytes)
GET_PT_TO_ASSET_RATE_SELECTOR = "0xabca0eab"  # getPtToAssetRate(address,uint32) on PendlePYLpOracle
GET_PT_TO_SY_RATE_SELECTOR = "0xa31426d1"  # getPtToSyRate(address,uint32) on PendlePYLpOracle
GET_ORACLE_STATE_SELECTOR = "0x873e9600"  # getOracleState(address,uint32) on PendlePYLpOracle
GET_IMPLIED_APY_SELECTOR = "0xfc0e022c"  # getImpliedApy(address) on RouterStatic (informational only)
READ_TOKENS_SELECTOR = "0x2c8ce6bc"  # readTokens() on the market contract (no-arg)
EXPIRY_SELECTOR = "0xe184c9be"  # expiry() on the market contract

# Minimal ABI for the PendlePYLpOracle read methods (only used in direct/web3 mode)
PT_ORACLE_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "market", "type": "address"},
            {"internalType": "uint32", "name": "duration", "type": "uint32"},
        ],
        "name": "getPtToAssetRate",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "market", "type": "address"},
            {"internalType": "uint32", "name": "duration", "type": "uint32"},
        ],
        "name": "getPtToSyRate",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "market", "type": "address"},
            {"internalType": "uint32", "name": "duration", "type": "uint32"},
        ],
        "name": "getOracleState",
        "outputs": [
            {"internalType": "bool", "name": "increaseCardinalityRequired", "type": "bool"},
            {"internalType": "uint16", "name": "cardinalityRequired", "type": "uint16"},
            {"internalType": "bool", "name": "oldestObservationSatisfied", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

# Minimal ABI for RouterStatic read methods (only used in direct/web3 mode).
# Only ``getImpliedApy`` remains — the rate read moved to the PT oracle and
# ``readTokens`` moved to the market contract.
ROUTER_STATIC_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "market", "type": "address"}],
        "name": "getImpliedApy",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# ABI for the market contract reads (expiry + readTokens). Only used in direct mode.
MARKET_ABI = [
    {
        "inputs": [],
        "name": "expiry",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "readTokens",
        "outputs": [
            {"internalType": "address", "name": "_SY", "type": "address"},
            {"internalType": "address", "name": "_PT", "type": "address"},
            {"internalType": "address", "name": "_YT", "type": "address"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

# Backwards-compatible alias (the expiry-only ABI used to live under this name).
MARKET_EXPIRY_ABI = MARKET_ABI

SCALE_1E18 = Decimal("1000000000000000000")


def _encode_address(addr: str) -> str:
    """ABI-encode an address as a 32-byte hex string (no 0x prefix)."""
    return addr.lower().removeprefix("0x").zfill(64)


def _encode_address_uint32(addr: str, value: int) -> str:
    """ABI-encode an (address, uint32) tuple as two 32-byte hex words (no 0x prefix)."""
    return _encode_address(addr) + format(value, "064x")


def _decode_uint256(hex_str: str) -> int:
    """Decode a hex string to a uint256."""
    raw = hex_str.removeprefix("0x")
    return int(raw, 16) if raw else 0


def _decode_address(hex_str: str) -> str:
    """Decode a 32-byte hex slot to a checksummed address."""
    raw = hex_str.removeprefix("0x")
    # Address is in the last 40 chars of a 64-char slot
    return "0x" + raw[-40:]


def _decode_oracle_state(hex_str: str) -> tuple[bool, int, bool]:
    """Decode ``getOracleState`` output into (increaseCardinalityRequired, cardinality, oldestObservationSatisfied)."""
    raw = hex_str.removeprefix("0x")
    if len(raw) < 192:
        raise PendleOnChainError(f"getOracleState returned unexpected data length: {len(raw)}")
    increase_cardinality_required = int(raw[0:64], 16) != 0
    cardinality = int(raw[64:128], 16)
    oldest_observation_satisfied = int(raw[128:192], 16) != 0
    return increase_cardinality_required, cardinality, oldest_observation_satisfied


class PendleOnChainError(Exception):
    """Raised when an on-chain read fails."""


class PendleOnChainReader:
    """Reads Pendle market data directly from on-chain contracts.

    Used as a fallback when the Pendle REST API is unavailable. The PT-to-asset
    rate is read from the per-chain PendlePYLpOracle (TWAP); ``readTokens`` /
    ``expiry`` are read from the market contract; ``getImpliedApy`` (informational
    only) is read from the RouterStatic when one is registered for the chain.

    Supports two initialization modes:

    Gateway mode (preferred for production):
        reader = PendleOnChainReader(gateway_client=client, chain="ethereum")

    Direct mode (legacy, local development):
        reader = PendleOnChainReader(rpc_url="https://...", chain="ethereum")
    """

    def __init__(
        self,
        rpc_url: str | None = None,
        chain: str = "ethereum",
        cache_ttl_seconds: float = 30.0,
        gateway_client: "GatewayClient | None" = None,
        request_timeout_seconds: float = 30.0,
    ):
        """Initialize the on-chain reader.

        Args:
            rpc_url: RPC endpoint URL (for direct/web3 mode).
            chain: Target chain (ethereum, arbitrum).
            cache_ttl_seconds: Cache TTL for on-chain reads.
            gateway_client: Gateway client (for gateway mode). Preferred over rpc_url.
            request_timeout_seconds: Bound on each blocking web3 RPC request in
                direct mode. Without it, ``web3`` defaults to no timeout, so a
                hung RPC wedges the caller (and, when the reader is driven from
                the gateway, a worker thread) indefinitely.

        Raises:
            ValueError: If chain is unsupported or neither rpc_url nor gateway_client provided.
        """
        if chain not in PT_ORACLE_ADDRESSES:
            raise ValueError(
                f"Unsupported chain for Pendle on-chain reads: {chain}. Supported: {list(PT_ORACLE_ADDRESSES.keys())}"
            )

        self.chain = chain
        self.cache_ttl = cache_ttl_seconds
        self.pt_oracle_address = PT_ORACLE_ADDRESSES[chain]
        # RouterStatic is optional (informational getImpliedApy only) and absent on
        # chains where Pendle decommissioned it (e.g. Arbitrum).
        self.router_static_address = ROUTER_STATIC_ADDRESSES.get(chain)
        self._gateway_client = gateway_client

        # Simple TTL cache
        self._cache: dict[str, tuple[Any, float]] = {}
        self._cache_lock = threading.Lock()

        if gateway_client is not None:
            # Gateway mode: no web3 dependency needed
            self.web3 = None
            self.pt_oracle = None
            self.router_static = None
            logger.info("PendleOnChainReader initialized (gateway mode): chain=%s", chain)
        elif rpc_url is not None:
            # Direct/web3 mode (HTTPProvider). Two distinct callers reach this branch
            # (both verifiable against source — references are file:symbol, grep them):
            #
            #   1. The GATEWAY ITSELF, gateway-side. ``GetPtPrice``
            #      (almanak/gateway/services/market_service.py: ``MarketService.GetPtPrice``)
            #      → ``_read_pt_market`` → ``_build_pt_reader`` →
            #      ``connector.build_principal_token_market_reader(chain, rpc_url)`` (same
            #      file) → ``PendleGatewayConnector.build_principal_token_market_reader``
            #      (almanak/connectors/pendle/gateway/provider.py), which returns THIS
            #      reader in direct (rpc_url) mode. The gateway holds RPC credentials
            #      (``gateway.utils.get_rpc_url``) and IS the egress layer, so this direct
            #      read is legitimate gateway-internal egress, NOT a strategy-container
            #      bypass. (NB: there is no ``almanak/gateway/`` reference to this reader
            #      because the binding lives in the connector's gateway subpackage
            #      ``pendle/gateway/provider.py`` — a scan limited to ``almanak/gateway/``
            #      misses it.) This consumer is what makes the direct branch genuinely
            #      load-bearing — deleting it breaks gateway PT pricing, not just local dev.
            #
            #   2. Strategy-container readers, ONLY when ``gateway_client is None``:
            #      ``adapter._get_on_chain_reader`` (this package) and PT health via
            #      ``framework/data/position_health`` →
            #      ``principal_token_registry.build_reader`` →
            #      ``on_chain_reader_provider.PendlePrincipalTokenMarketReadConnector``.
            #
            # Consumer (2) is UNREACHABLE from a hosted strategy container: the runner
            # always wires a connected gateway_client, so ``intents/compiler._get_chain_rpc_url``
            # returns None and ``pendle/compiler._resolve_pendle_adapter_inputs`` forces
            # ``rpc_url=None`` → the gateway branch above is taken and no HTTPProvider is
            # built. Proven by ``tests/reports/pendle_egress_trace_vib5305.md`` and the
            # ``TestHostedStrategyContainerNoHttpProvider`` regression guards (which also
            # pin the compiler decision that yields ``rpc_url=None``).
            #
            # Final removal of BOTH consumers' direct path is tracked by VIB-5348
            # (debt origin: VIB-2986).
            from web3 import Web3

            self.web3 = Web3(
                Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": request_timeout_seconds})
            )  # vib-2986-exempt: gateway-internal + local-dev fallback, removal tracked by VIB-5348
            self.pt_oracle = self.web3.eth.contract(
                address=self.web3.to_checksum_address(self.pt_oracle_address),
                abi=PT_ORACLE_ABI,
            )
            self.router_static = (
                self.web3.eth.contract(
                    address=self.web3.to_checksum_address(self.router_static_address),
                    abi=ROUTER_STATIC_ABI,
                )
                if self.router_static_address is not None
                else None
            )
            logger.info("PendleOnChainReader initialized (direct mode): chain=%s", chain)
        else:
            raise ValueError("Either rpc_url or gateway_client must be provided")

    def _gateway_eth_call(self, to: str, data: str, request_id: str) -> str:
        """Make an eth_call through the gateway's RPC service.

        Args:
            to: Contract address.
            data: Encoded calldata (hex string with 0x prefix).
            request_id: Request identifier for logging.

        Returns:
            Hex result string from the call.

        Raises:
            PendleOnChainError: If the RPC call fails.
        """
        from almanak.gateway.proto import gateway_pb2

        assert self._gateway_client is not None  # guaranteed by callers

        params = json.dumps([{"to": to, "data": data}, "latest"])
        try:
            resp = self._gateway_client.rpc.Call(
                gateway_pb2.RpcRequest(
                    chain=self.chain,
                    method="eth_call",
                    params=params,
                    id=request_id,
                ),
                timeout=30.0,
            )
        except Exception as e:
            raise PendleOnChainError(f"Gateway RPC call failed ({request_id}): {e}") from e

        if not resp.success:
            raise PendleOnChainError(f"Gateway RPC call error ({request_id}): {resp.error}")

        result = json.loads(resp.result)
        if result is None or result == "0x":
            raise PendleOnChainError(f"Empty result from gateway RPC ({request_id})")
        return result

    def _read_oracle_state(self, market_address: str) -> tuple[bool, int, bool]:
        """Read ``getOracleState(market, duration)`` from the PT oracle.

        Returns ``(increaseCardinalityRequired, cardinality, oldestObservationSatisfied)``.

        Raises:
            PendleOnChainError: If the RPC call fails or reverts. An old wound-down
                market reverts ``getOracleState`` on real mainnet (VIB-5352); the
                raw web3 / gateway exception is wrapped here so the typed-error
                contract holds for EVERY caller. This matters because
                :meth:`_assert_oracle_ready` runs in :meth:`get_pt_to_asset_rate`
                BEFORE that method's own try/except, so an unwrapped raw exception
                would otherwise leak past the documented ``PendleOnChainError``
                surface that callers rely on to emit UNMEASURED.
        """
        try:
            if self._gateway_client is not None:
                calldata = GET_ORACLE_STATE_SELECTOR + _encode_address_uint32(
                    market_address, PT_ORACLE_TWAP_DURATION_SECONDS
                )
                result = self._gateway_eth_call(self.pt_oracle_address, calldata, "pendle_oracle_state")
                return _decode_oracle_state(result)
            assert self.web3 is not None and self.pt_oracle is not None
            increase_required, cardinality, oldest_ok = self.pt_oracle.functions.getOracleState(
                self.web3.to_checksum_address(market_address),
                PT_ORACLE_TWAP_DURATION_SECONDS,
            ).call()
            return bool(increase_required), int(cardinality), bool(oldest_ok)
        except PendleOnChainError:
            raise
        except Exception as e:
            logger.warning("Failed to read PT oracle state for %s: %s", market_address, e)
            raise PendleOnChainError(f"getOracleState failed: {e}") from e

    def _assert_oracle_ready(self, market_address: str) -> None:
        """Gate the rate read on TWAP readiness — Empty≠Zero.

        A PT oracle that cannot yet produce a ``duration``-second TWAP must yield
        NO number, never a fabricated at-par (1.0) rate that would overvalue the PT
        to its maximum redemption value (PT trades at ≤ par before maturity).

        Gate semantics (intentionally NOT a blanket both-flags gate):

        - ``oldestObservationSatisfied == False`` → the observation window cannot be
          spanned; the rate read itself would revert. Treated as UNMEASURED (raise).
        - ``increaseCardinalityRequired == True`` → forward-looking buffer-size advice
          ONLY. The current read is still valid — verified on the live Ethereum
          production market (Aug-2026 sUSDe), which reports this flag ``True`` yet
          returns a correct rate matching the legacy RouterStatic spot read. It is
          logged as a warning but NOT treated as UNMEASURED; gating on it would zero
          out an otherwise-valid PT valuation (a regression worse than the bug).

        Raises:
            PendleOnChainError: When the oracle is not ready (→ caller emits UNMEASURED).
        """
        increase_required, cardinality, oldest_ok = self._read_oracle_state(market_address)
        if not oldest_ok:
            raise PendleOnChainError(
                f"PT oracle not ready for {market_address}: oldestObservationSatisfied=False "
                f"(duration={PT_ORACLE_TWAP_DURATION_SECONDS}s) — UNMEASURED, not fabricated"
            )
        if increase_required:
            logger.warning(
                "PT oracle for %s reports increaseCardinalityRequired=True (cardinality=%s); "
                "current TWAP read is still valid, but the observation buffer should be grown",
                market_address,
                cardinality,
            )

    def get_pt_to_asset_rate(self, market_address: str) -> Decimal:
        """Get the PT-to-**accounting-asset** exchange rate via the PT oracle TWAP.

        Returns how much of the SY's *accounting asset* 1 PT is worth. NOTE the
        denomination trap (VIB-5407): the SY accounting asset is NOT necessarily
        the SY's mint/underlying token. For a wrapped-staking SY (e.g. SY-wstETH
        whose accounting asset is stETH), ``getPtToAssetRate`` bakes in the
        wstETH→stETH wrap accretion, so it converges toward ~1.0 well before
        maturity and OVER-marks the PT versus its discounted market price when the
        gateway prices the mint/underlying token (wstETH). It is therefore **NOT
        the money-path mark** — use :meth:`get_pt_to_sy_rate` for valuation. This
        read is retained for transparency/health (implied-APR context) only.

        Read from the per-chain PendlePYLpOracle using the 2-arg
        ``getPtToAssetRate(market, duration)`` TWAP call (``duration`` fixed at
        ``PT_ORACLE_TWAP_DURATION_SECONDS``). The read is gated on oracle readiness
        (:meth:`_assert_oracle_ready`) so a not-ready oracle surfaces as UNMEASURED
        rather than a fabricated rate.

        Args:
            market_address: Market contract address

        Returns:
            Exchange rate as Decimal (in 1e18 scale, normalized to human-readable)

        Raises:
            PendleOnChainError: If the oracle is not ready or the RPC call fails
        """
        cache_key = f"pt_rate:{market_address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # Readiness gate first — never cache or fabricate when UNMEASURED.
        self._assert_oracle_ready(market_address)

        try:
            if self._gateway_client is not None:
                calldata = GET_PT_TO_ASSET_RATE_SELECTOR + _encode_address_uint32(
                    market_address, PT_ORACLE_TWAP_DURATION_SECONDS
                )
                result = self._gateway_eth_call(self.pt_oracle_address, calldata, "pendle_pt_rate")
                raw_rate = _decode_uint256(result)
            else:
                assert self.web3 is not None and self.pt_oracle is not None
                raw_rate = self.pt_oracle.functions.getPtToAssetRate(
                    self.web3.to_checksum_address(market_address),
                    PT_ORACLE_TWAP_DURATION_SECONDS,
                ).call()
            rate = Decimal(str(raw_rate)) / SCALE_1E18
            self._set_cached(cache_key, rate)
            return rate
        except PendleOnChainError:
            raise
        except Exception as e:
            logger.warning("Failed to read PT-to-asset rate for %s: %s", market_address, e)
            raise PendleOnChainError(f"getPtToAssetRate failed: {e}") from e

    def get_pt_to_sy_rate(self, market_address: str) -> Decimal:
        """Get the PT-to-**SY** exchange rate via the PT oracle TWAP (the money mark).

        Returns how many SY units 1 PT is currently worth — i.e. the PT's
        **discounted market price** in SY terms. Because the gateway prices the
        SY's mint/underlying token (which the SY wraps ~1:1, e.g. wstETH for
        SY-wstETH), ``PT/USD = getPtToSyRate × underlying/USD`` is the honest
        market mark a holder would realize selling the PT now.

        This is the canonical money-path rate for open-PT mark-to-market
        (VIB-5407), correcting :meth:`get_pt_to_asset_rate`, which is denominated
        in the SY *accounting asset* (e.g. stETH) and therefore over-marks the PT
        to ~par when the priced underlying is the wrap token (wstETH). The two
        rates differ by exactly the SY wrap-accretion (``SY.exchangeRate()``); they
        coincide only when the mint token equals the accounting asset 1:1.

        Read from the per-chain PendlePYLpOracle using the 2-arg
        ``getPtToSyRate(market, duration)`` TWAP call (same oracle, same
        ``PT_ORACLE_TWAP_DURATION_SECONDS`` window). Gated on oracle readiness
        (:meth:`_assert_oracle_ready`) so a not-ready oracle surfaces as
        UNMEASURED rather than a fabricated rate (Empty≠Zero).

        Args:
            market_address: Market contract address

        Returns:
            PT/SY exchange rate as Decimal (1e18 scale normalized to human-readable)

        Raises:
            PendleOnChainError: If the oracle is not ready or the RPC call fails
        """
        cache_key = f"pt_sy_rate:{market_address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # Readiness gate first — never cache or fabricate when UNMEASURED.
        self._assert_oracle_ready(market_address)

        try:
            if self._gateway_client is not None:
                calldata = GET_PT_TO_SY_RATE_SELECTOR + _encode_address_uint32(
                    market_address, PT_ORACLE_TWAP_DURATION_SECONDS
                )
                result = self._gateway_eth_call(self.pt_oracle_address, calldata, "pendle_pt_sy_rate")
                raw_rate = _decode_uint256(result)
            else:
                assert self.web3 is not None and self.pt_oracle is not None
                raw_rate = self.pt_oracle.functions.getPtToSyRate(
                    self.web3.to_checksum_address(market_address),
                    PT_ORACLE_TWAP_DURATION_SECONDS,
                ).call()
            rate = Decimal(str(raw_rate)) / SCALE_1E18
            self._set_cached(cache_key, rate)
            return rate
        except PendleOnChainError:
            raise
        except Exception as e:
            logger.warning("Failed to read PT-to-SY rate for %s: %s", market_address, e)
            raise PendleOnChainError(f"getPtToSyRate failed: {e}") from e

    def get_implied_apy(self, market_address: str) -> Decimal:
        """Get the implied APY for a market.

        Args:
            market_address: Market contract address

        Returns:
            Implied APY as Decimal (e.g., 0.05 = 5%)

        Raises:
            PendleOnChainError: If the RPC call fails
        """
        cache_key = f"implied_apy:{market_address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        if self.router_static_address is None:
            # Informational read only; no RouterStatic on this chain (e.g. Arbitrum,
            # where Pendle decommissioned it). Surface cleanly so callers degrade.
            raise PendleOnChainError(f"getImpliedApy unavailable on {self.chain}: no RouterStatic registered")

        try:
            if self._gateway_client is not None:
                calldata = GET_IMPLIED_APY_SELECTOR + _encode_address(market_address)
                result = self._gateway_eth_call(self.router_static_address, calldata, "pendle_implied_apy")
                raw_apy = _decode_uint256(result)
            else:
                assert self.web3 is not None and self.router_static is not None
                raw_apy = self.router_static.functions.getImpliedApy(
                    self.web3.to_checksum_address(market_address)
                ).call()
            apy = Decimal(str(raw_apy)) / SCALE_1E18
            self._set_cached(cache_key, apy)
            return apy
        except PendleOnChainError:
            raise
        except Exception as e:
            logger.warning("Failed to read implied APY for %s: %s", market_address, e)
            raise PendleOnChainError(f"getImpliedApy failed: {e}") from e

    def get_market_expiry_ts(self, market_address: str) -> int | None:
        """Return the market's on-chain ``expiry()`` as a unix timestamp, or None.

        Single source of truth for the PT maturity timestamp: ``is_market_expired``
        and ``get_days_to_maturity`` both derive from this read so a caller that
        needs the raw timestamp, days-remaining, and the expired flag observes one
        consistent expiry (VIB-5384).

        ``expiry()`` reads the MARKET contract, not RouterStatic — do not gate on
        ``router_static`` (absent on Arbitrum).

        Never raises — a failed read returns ``None`` (Empty≠Zero: the caller maps
        an unread expiry to an unmeasured maturity, never a fabricated 0). The
        result is cached so the expired / days / timestamp views stay coherent
        within the cache window.

        Args:
            market_address: Pendle market contract address.

        Returns:
            Unix-seconds expiry timestamp, or ``None`` if it could not be read.
        """
        cache_key = f"expiry_ts:{market_address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            if self._gateway_client is not None:
                result = self._gateway_eth_call(market_address, EXPIRY_SELECTOR, "pendle_expiry")
                expiry = _decode_uint256(result)
            else:
                # expiry() reads the MARKET contract, not RouterStatic — do not gate
                # on router_static (absent on Arbitrum).
                assert self.web3 is not None
                market_contract = self.web3.eth.contract(
                    address=self.web3.to_checksum_address(market_address),
                    abi=MARKET_EXPIRY_ABI,
                )
                expiry = market_contract.functions.expiry().call()
        except Exception as e:
            logger.debug("pendle: get_market_expiry_ts failed for %s: %s", market_address, e)
            return None

        expiry = int(expiry)
        if expiry <= 0:
            # A non-positive expiry is not a real on-chain maturity. Treat it as
            # unread rather than caching/propagating a fabricated 0 (Empty≠Zero).
            logger.debug("pendle: get_market_expiry_ts read non-positive expiry %s for %s", expiry, market_address)
            return None
        self._set_cached(cache_key, expiry)
        return expiry

    def is_market_expired(self, market_address: str) -> bool:
        """Check if a market has expired.

        Args:
            market_address: Market contract address

        Returns:
            True if the market has expired

        Raises:
            PendleOnChainError: If the RPC call fails
        """
        cache_key = f"expiry:{market_address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        expiry = self.get_market_expiry_ts(market_address)
        if expiry is None:
            raise PendleOnChainError(f"expiry() failed for {market_address}")
        is_expired = int(time.time()) >= expiry
        self._set_cached(cache_key, is_expired)
        return is_expired

    def get_days_to_maturity(self, market_address: str) -> int | None:
        """Return calendar days remaining until PT maturity, or None on failure.

        Returns 0 when the market is already expired.  Never raises — a failed
        expiry read returns ``None`` so callers can treat the absence of this data
        as non-fatal. Derived from the same ``get_market_expiry_ts`` read as the
        raw timestamp so the two stay consistent (VIB-5384).

        Args:
            market_address: Pendle market contract address.

        Returns:
            Non-negative integer days to maturity, or None if the expiry
            timestamp could not be read.
        """
        import math as _math

        expiry = self.get_market_expiry_ts(market_address)
        if expiry is None:
            return None
        now_ts = int(time.time())
        return max(0, _math.ceil((expiry - now_ts) / 86400))

    def get_market_tokens(self, market_address: str) -> dict[str, str]:
        """Get SY, PT, and YT addresses for a market.

        Read from the **market** contract's no-arg ``readTokens()`` (chain-portable;
        no RouterStatic dependency).

        Args:
            market_address: Market contract address

        Returns:
            Dict with keys "sy", "pt", "yt" mapping to addresses

        Raises:
            PendleOnChainError: If the RPC call fails
        """
        cache_key = f"tokens:{market_address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            if self._gateway_client is not None:
                raw = self._gateway_eth_call(market_address, READ_TOKENS_SELECTOR, "pendle_read_tokens")
                hex_data = raw.removeprefix("0x")
                if len(hex_data) < 192:
                    raise PendleOnChainError(f"readTokens returned unexpected data length: {len(hex_data)}")
                result = {
                    "sy": _decode_address(hex_data[0:64]),
                    "pt": _decode_address(hex_data[64:128]),
                    "yt": _decode_address(hex_data[128:192]),
                }
            else:
                assert self.web3 is not None
                market_contract = self.web3.eth.contract(
                    address=self.web3.to_checksum_address(market_address),
                    abi=MARKET_ABI,
                )
                sy, pt, yt = market_contract.functions.readTokens().call()
                result = {
                    "sy": sy.lower(),
                    "pt": pt.lower(),
                    "yt": yt.lower(),
                }
            self._set_cached(cache_key, result)
            return result
        except PendleOnChainError:
            raise
        except Exception as e:
            logger.warning("Failed to read market tokens for %s: %s", market_address, e)
            raise PendleOnChainError(f"readTokens failed: {e}") from e

    def estimate_pt_output(self, market_address: str, amount_in: int) -> int:
        """Estimate PT output for a given input amount using the on-chain rate.

        Args:
            market_address: Market contract address
            amount_in: Input amount in wei

        Returns:
            Estimated PT output in wei
        """
        rate = self.get_pt_to_asset_rate(market_address)
        if rate <= 0:
            raise PendleOnChainError(f"Invalid PT rate for {market_address}: {rate}")
        # amount_out = amount_in / rate (since rate is asset-per-PT)
        return int(Decimal(str(amount_in)) / rate)

    # =========================================================================
    # Cache
    # =========================================================================

    def _get_cached(self, key: str) -> Any | None:
        with self._cache_lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            value, expiry = entry
            if time.monotonic() > expiry:
                del self._cache[key]
                return None
            return value

    def _set_cached(self, key: str, value: Any) -> None:
        with self._cache_lock:
            self._cache[key] = (value, time.monotonic() + self.cache_ttl)

    def clear_cache(self) -> None:
        with self._cache_lock:
            self._cache.clear()
