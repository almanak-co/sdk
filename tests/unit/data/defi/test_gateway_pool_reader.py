"""Unit tests for GatewayPoolReserveReader branch behaviour (VIB-4845).

The wiring tests in ``tests/framework/market/test_rpc_grpc_provider_wiring_vib4845.py``
exercise the happy live path through the builder. These pin the reader's
*branchy* behaviour directly (CR1 / multi-auditor review of PR #2555):

- resolver hit (registry decimals) vs miss (on-chain ``decimals()`` fallback);
- the DoS guard: an oversized / malicious ``decimals()`` response must fail loud
  as ``DataUnavailableError`` rather than feeding ``10**decimals`` a huge int;
- the error contract: a failed chain read surfaces as ``DataUnavailableError``
  (transient), not a generic ``DataSourceError``;
- TVL best-effort fallbacks: no oracle, unresolved symbol, and a sync oracle
  returning ``None`` all yield ``Decimal("0")`` without crashing.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.data.defi.gateway_pool_reader import (
    _MAX_REASONABLE_DECIMALS,
    GatewayPoolReserveReader,
)
from almanak.framework.data.exceptions import DataUnavailableError

# Selectors mirror the production reader modules.
_SLOT0 = "0x3850c7bd"
_LIQUIDITY = "0x1a686502"
_TOKEN0 = "0x0dfe1681"
_TOKEN1 = "0xd21220a7"
_FEE = "0xddca3f43"
_DECIMALS = "0x313ce567"
_BALANCE_OF = "0x70a08231"

_TOKEN0_ADDR = "0x1111111111111111111111111111111111111111"  # token0 (lower addr)
_TOKEN1_ADDR = "0x2222222222222222222222222222222222222222"
_POOL = "0x3333333333333333333333333333333333333333"

_SQRT_PRICE_X96 = 79228162514264337593543950336  # 2**96
_TICK = -100
_RESERVE0_RAW = 5_000_000_000  # 5,000 @ 6 decimals
_RESERVE1_RAW = 3_000_000_000_000_000_000  # 3 @ 18 decimals


def _word(value: int, *, signed: bool = False) -> str:
    return value.to_bytes(32, byteorder="big", signed=signed).hex()


def _addr_word(addr: str) -> str:
    return addr.lower().removeprefix("0x").zfill(64)


def _to_bytes(hexstr: str) -> bytes:
    if not hexstr or hexstr == "0x":
        return b""
    return bytes.fromhex(hexstr.removeprefix("0x"))


def _make_rpc_call(*, decimals0: int = 6, decimals1: int = 18, overrides: dict | None = None):
    """Return a ``(chain, to, data) -> bytes`` rpc_call crafting pool/token reads.

    ``overrides`` maps a selector to either a hex string (returned as bytes) or an
    ``Exception`` instance (raised) — used to inject malformed / failing reads.
    """
    overrides = overrides or {}

    def _rpc_call(chain: str, to: str, data: str) -> bytes:  # noqa: ARG001
        selector = data[:10].lower()
        to_l = to.lower()
        if selector in overrides:
            ov = overrides[selector]
            if isinstance(ov, Exception):
                raise ov
            return _to_bytes(ov)
        if selector == _SLOT0:
            return _to_bytes("0x" + _word(_SQRT_PRICE_X96) + _word(_TICK, signed=True) + _word(0) * 5)
        if selector == _LIQUIDITY:
            return _to_bytes("0x" + _word(987654321))
        if selector == _TOKEN0:
            return _to_bytes("0x" + _addr_word(_TOKEN0_ADDR))
        if selector == _TOKEN1:
            return _to_bytes("0x" + _addr_word(_TOKEN1_ADDR))
        if selector == _FEE:
            return _to_bytes("0x" + _word(500))
        if selector == _DECIMALS:
            return _to_bytes("0x" + _word(decimals0 if to_l == _TOKEN0_ADDR else decimals1))
        if selector == _BALANCE_OF:
            return _to_bytes("0x" + _word(_RESERVE0_RAW if to_l == _TOKEN0_ADDR else _RESERVE1_RAW))
        return b""

    return _rpc_call


class _Resolver:
    """Registry-backed resolver: token0 always known; token1 optionally known."""

    def __init__(self, *, resolve_token1: bool = True):
        self._resolve_token1 = resolve_token1

    def resolve(self, token_address: str, chain: str, log_errors: bool = False):  # noqa: ARG002
        if token_address.lower() == _TOKEN0_ADDR:
            return SimpleNamespace(symbol="USDC", name="USD Coin", decimals=6)
        if token_address.lower() == _TOKEN1_ADDR and self._resolve_token1:
            return SimpleNamespace(symbol="WETH", name="Wrapped Ether", decimals=18)
        raise KeyError(token_address)


def _read(reader: GatewayPoolReserveReader):
    # Use a dedicated loop instead of asyncio.run(): asyncio.run() sets the
    # current event loop to None on exit, which breaks later tests on the same
    # xdist worker that still use the deprecated asyncio.get_event_loop() pattern
    # (e.g. tests/framework/observability/test_snapshot_accounting.py). new_event_loop()
    # does not touch the policy's current loop, so this leaves global state intact.
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(reader.get_pool_reserves(_POOL, "base"))
    finally:
        loop.close()


# --------------------------------------------------------------------------- #
# decimals resolution: registry hit vs on-chain fallback
# --------------------------------------------------------------------------- #


def test_resolver_hit_uses_registry_decimals_no_onchain_call():
    seen: list[str] = []
    base = _make_rpc_call()

    def rpc_call(chain, to, data):
        seen.append(data[:10].lower())
        return base(chain, to, data)

    reserves = _read(GatewayPoolReserveReader(rpc_call=rpc_call, token_resolver=_Resolver()))
    assert reserves.token0.decimals == 6
    assert reserves.token1.decimals == 18
    assert reserves.reserve0 == Decimal("5000")
    assert reserves.reserve1 == Decimal("3")
    # Both tokens resolved from the registry -> NO decimals() eth_call issued.
    assert _DECIMALS not in seen


def test_resolver_miss_falls_back_to_onchain_decimals():
    seen: list[tuple[str, str]] = []
    base = _make_rpc_call(decimals1=18)

    def rpc_call(chain, to, data):
        seen.append((data[:10].lower(), to.lower()))
        return base(chain, to, data)

    reserves = _read(GatewayPoolReserveReader(rpc_call=rpc_call, token_resolver=_Resolver(resolve_token1=False)))
    assert reserves.token1.decimals == 18  # from the on-chain read
    assert reserves.reserve1 == Decimal("3")
    assert (_DECIMALS, _TOKEN1_ADDR) in seen  # decimals() WAS read on-chain for token1


def test_no_resolver_reads_decimals_onchain_for_both():
    reserves = _read(GatewayPoolReserveReader(rpc_call=_make_rpc_call(), token_resolver=None))
    assert reserves.token0.decimals == 6
    assert reserves.token1.decimals == 18
    assert reserves.token0.symbol == "UNKNOWN"


# --------------------------------------------------------------------------- #
# DoS guard: oversized / malicious decimals() (multi-auditor blocker)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("bad_decimals", [_MAX_REASONABLE_DECIMALS + 1, 255, 2**200])
def test_oversized_decimals_raises_data_unavailable(bad_decimals):
    # token1 unresolved -> on-chain decimals() path; craft a hostile value.
    reader = GatewayPoolReserveReader(
        rpc_call=_make_rpc_call(overrides={_DECIMALS: "0x" + _word(bad_decimals)}),
        token_resolver=_Resolver(resolve_token1=False),
    )
    with pytest.raises(DataUnavailableError, match="decimals"):
        _read(reader)


# --------------------------------------------------------------------------- #
# error contract: transient chain-read failures -> DataUnavailableError
# --------------------------------------------------------------------------- #


def test_rpc_failure_raises_data_unavailable():
    reader = GatewayPoolReserveReader(
        rpc_call=_make_rpc_call(overrides={_SLOT0: RuntimeError("gateway boom")}),
        token_resolver=_Resolver(),
    )
    with pytest.raises(DataUnavailableError, match="chain read failed"):
        _read(reader)


def test_short_payload_raises_data_unavailable():
    # A truncated slot0 word cannot be decoded -> decoder raises DataUnavailableError.
    reader = GatewayPoolReserveReader(
        rpc_call=_make_rpc_call(overrides={_SLOT0: "0x1234"}),
        token_resolver=_Resolver(),
    )
    with pytest.raises(DataUnavailableError):
        _read(reader)


# --------------------------------------------------------------------------- #
# resolver-provided decimals are validated too (not just the on-chain read)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("bad_resolver_decimals", [-1, 9999, _MAX_REASONABLE_DECIMALS + 1])
def test_invalid_resolver_decimals_falls_back_to_onchain(bad_resolver_decimals):
    class _BadDecimalsResolver:
        def resolve(self, token_address, chain, log_errors=False):  # noqa: ARG002
            # token0 fine; token1 returns an out-of-range registry decimals.
            if token_address.lower() == _TOKEN0_ADDR:
                return SimpleNamespace(symbol="USDC", name="USD Coin", decimals=6)
            return SimpleNamespace(symbol="WETH", name="Wrapped Ether", decimals=bad_resolver_decimals)

    seen: list[tuple[str, str]] = []
    base = _make_rpc_call(decimals1=18)  # on-chain decimals() returns the real 18

    def rpc_call(chain, to, data):
        seen.append((data[:10].lower(), to.lower()))
        return base(chain, to, data)

    reserves = _read(GatewayPoolReserveReader(rpc_call=rpc_call, token_resolver=_BadDecimalsResolver()))
    # The bad registry value is rejected -> on-chain decimals() is consulted.
    assert reserves.token1.decimals == 18
    assert reserves.reserve1 == Decimal("3")
    assert (_DECIMALS, _TOKEN1_ADDR) in seen


def test_invalid_resolver_decimals_with_bad_onchain_raises():
    class _BadDecimalsResolver:
        def resolve(self, token_address, chain, log_errors=False):  # noqa: ARG002
            if token_address.lower() == _TOKEN0_ADDR:
                return SimpleNamespace(symbol="USDC", name="USD Coin", decimals=6)
            return SimpleNamespace(symbol="WETH", name="Wrapped Ether", decimals=9999)

    # Registry decimals invalid AND on-chain decimals() also hostile -> fail loud.
    reader = GatewayPoolReserveReader(
        rpc_call=_make_rpc_call(overrides={_DECIMALS: "0x" + _word(9999)}),
        token_resolver=_BadDecimalsResolver(),
    )
    with pytest.raises(DataUnavailableError, match="decimals"):
        _read(reader)


# --------------------------------------------------------------------------- #
# TVL best-effort: oracle present / absent / unresolved symbol / price None
# --------------------------------------------------------------------------- #


class _AsyncOracle:
    def __init__(self, prices: dict[str, Decimal]):
        self._prices = prices
        self.calls = 0

    async def get_aggregated_price(self, token: str, quote: str = "USD", *, chain: str | None = None):
        self.calls += 1
        return SimpleNamespace(price=self._prices[token])


def test_tvl_computed_from_oracle():
    reader = GatewayPoolReserveReader(
        rpc_call=_make_rpc_call(),
        token_resolver=_Resolver(),
        price_oracle=_AsyncOracle({"USDC": Decimal("1"), "WETH": Decimal("3000")}),
    )
    # 5000 USDC * 1 + 3 WETH * 3000 = 14000
    assert _read(reader).tvl_usd == Decimal("14000")


def test_tvl_zero_without_oracle():
    reader = GatewayPoolReserveReader(rpc_call=_make_rpc_call(), token_resolver=_Resolver())
    assert _read(reader).tvl_usd == Decimal("0")


def test_tvl_zero_and_unpriced_when_symbol_unknown():
    oracle = _AsyncOracle({"USDC": Decimal("1")})
    # token1 unresolved -> symbol "UNKNOWN" -> oracle must NOT be called.
    reader = GatewayPoolReserveReader(
        rpc_call=_make_rpc_call(),
        token_resolver=_Resolver(resolve_token1=False),
        price_oracle=oracle,
    )
    assert _read(reader).tvl_usd == Decimal("0")
    assert oracle.calls == 0


def test_tvl_zero_when_oracle_price_is_none():
    class _NonePriceOracle:
        async def get_aggregated_price(self, token: str, quote: str = "USD", *, chain: str | None = None):  # noqa: ARG002
            return SimpleNamespace(price=None)  # price miss — must not become Decimal("None")

    reader = GatewayPoolReserveReader(
        rpc_call=_make_rpc_call(),
        token_resolver=_Resolver(),
        price_oracle=_NonePriceOracle(),
    )
    assert _read(reader).tvl_usd == Decimal("0")


def test_tvl_zero_when_oracle_lacks_aggregated_price():
    # An injected object without the documented async get_aggregated_price cannot
    # price TVL — best-effort 0 (no sync get_price guessing).
    class _WrongShapeOracle:
        def get_price(self, token: str, quote: str = "USD"):  # noqa: ARG002
            return Decimal("1")

    reader = GatewayPoolReserveReader(
        rpc_call=_make_rpc_call(),
        token_resolver=_Resolver(),
        price_oracle=_WrongShapeOracle(),
    )
    assert _read(reader).tvl_usd == Decimal("0")
