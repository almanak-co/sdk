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
    # A transient failure AFTER shape detection (liquidity() here) surfaces as
    # the classic "chain read failed" transient error.
    reader = GatewayPoolReserveReader(
        rpc_call=_make_rpc_call(overrides={_LIQUIDITY: RuntimeError("gateway boom")}),
        token_resolver=_Resolver(),
    )
    with pytest.raises(DataUnavailableError, match="chain read failed"):
        _read(reader)


def test_slot0_failure_falls_through_and_reports_both_probe_misses():
    # slot0() failing on an unknown-shape pool falls through to the
    # getReserves() probe; when that also misses, the error reports both,
    # including the original slot0 failure for debuggability.
    reader = GatewayPoolReserveReader(
        rpc_call=_make_rpc_call(overrides={_SLOT0: RuntimeError("gateway boom")}),
        token_resolver=_Resolver(),
    )
    with pytest.raises(DataUnavailableError, match="neither slot0.*gateway boom"):
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


# --------------------------------------------------------------------------- #
# Solidly (Aerodrome / Velodrome) and plain-V2 pools: shape auto-detection
# --------------------------------------------------------------------------- #

_GET_RESERVES = "0x0902f1ac"
_STABLE = "0x22be3de1"
_FACTORY = "0xc45a0155"
_GET_FEE = "0xcc56b2c5"
_PRICE0_CUMULATIVE = "0x5909c0d5"

_FACTORY_ADDR = "0x4444444444444444444444444444444444444444"

# Real Aerodrome default fees (basis points): volatile 30 (0.30%), stable 5 (0.05%).
_VOLATILE_FEE_BPS = 30
_STABLE_FEE_BPS = 5


def _make_solidly_rpc_call(
    *,
    stable: bool | None = False,
    slot0_behaviour: str | Exception = "empty",
    overrides: dict | None = None,
):
    """rpc_call for a getReserves-shaped pool. Returns the closure; it records
    every ``(selector, to)`` pair on ``.calls`` for post-read assertions.

    ``stable=None`` models a plain V2 pair: ``stable()`` reverts (empty) and
    the canonical V2 oracle getter ``price0CumulativeLast()`` answers.
    ``slot0_behaviour``: "empty" returns b"" (gateway maps a revert to "0x");
    an Exception instance is raised (gateway surfaces the revert as an error).

    The ``getFee`` handler decodes the calldata and answers from the encoded
    stable flag (volatile 30 bps, stable 5 bps), so a regression that flips or
    drops the stable word — or requests the fee for the wrong pool — returns
    the wrong fee and fails the fee assertions.
    """
    overrides = overrides or {}
    calls: list[tuple[str, str]] = []

    def _rpc_call(chain: str, to: str, data: str) -> bytes:  # noqa: ARG001
        selector = data[:10].lower()
        to_l = to.lower()
        calls.append((selector, to_l))
        if selector in overrides:
            ov = overrides[selector]
            if isinstance(ov, Exception):
                raise ov
            return _to_bytes(ov)
        if selector == _SLOT0:
            if isinstance(slot0_behaviour, Exception):
                raise slot0_behaviour
            return b""
        if selector == _GET_RESERVES:
            return _to_bytes("0x" + _word(_RESERVE0_RAW) + _word(_RESERVE1_RAW) + _word(1_700_000_000))
        if selector == _STABLE:
            if stable is None:
                return b""  # plain V2 pair: stable() reverts
            return _to_bytes("0x" + _word(1 if stable else 0))
        if selector == _PRICE0_CUMULATIVE:
            if stable is None:
                return _to_bytes("0x" + _word(123456789))  # V2 oracle getter answers
            return b""  # Solidly pools expose reserve0CumulativeLast instead
        if selector == _FACTORY:
            return _to_bytes("0x" + _addr_word(_FACTORY_ADDR))
        if selector == _GET_FEE:
            if to_l != _FACTORY_ADDR:
                return b""  # fee must be requested from the pool's factory
            pool_word, stable_word = data[10:74], data[74:138]
            if pool_word != _addr_word(_POOL):
                return b""  # fee requested for the wrong pool
            fee_bps = _STABLE_FEE_BPS if int(stable_word, 16) else _VOLATILE_FEE_BPS
            return _to_bytes("0x" + _word(fee_bps))
        if selector == _TOKEN0:
            return _to_bytes("0x" + _addr_word(_TOKEN0_ADDR))
        if selector == _TOKEN1:
            return _to_bytes("0x" + _addr_word(_TOKEN1_ADDR))
        if selector == _DECIMALS:
            return _to_bytes("0x" + _word(6 if to_l == _TOKEN0_ADDR else 18))
        return b""

    _rpc_call.calls = calls
    return _rpc_call


def _flaky_once(base, selector: str, behaviour):
    """Wrap ``base`` so the FIRST call hitting ``selector`` misbehaves
    (raise an Exception or return the given hex), then passes through."""
    fired = {"done": False}

    def _rpc_call(chain, to, data):
        if data[:10].lower() == selector and not fired["done"]:
            fired["done"] = True
            if isinstance(behaviour, Exception):
                raise behaviour
            return _to_bytes(behaviour)
        return base(chain, to, data)

    return _rpc_call


def test_solidly_volatile_pool_reads_via_get_reserves():
    reader = GatewayPoolReserveReader(
        rpc_call=_make_solidly_rpc_call(stable=False),
        token_resolver=_Resolver(),
    )
    reserves = _read(reader)
    assert reserves.dex == "solidly_v2"
    assert reserves.stable is False
    assert reserves.fee_tier == 3000  # 30 bps -> hundredths-of-a-bps units (0.30%)
    assert reserves.reserve0 == Decimal("5000")
    assert reserves.reserve1 == Decimal("3")
    # V3-only fields stay unset — Empty != Zero.
    assert reserves.sqrt_price_x96 is None
    assert reserves.tick is None
    assert reserves.liquidity is None


def test_solidly_stable_pool_reads_stable_flag_and_fee():
    rpc = _make_solidly_rpc_call(stable=True)
    reserves = _read(GatewayPoolReserveReader(rpc_call=rpc, token_resolver=_Resolver()))
    assert reserves.dex == "solidly_v2"
    assert reserves.stable is True
    assert reserves.fee_tier == 500  # 5 bps stable fee, taken from the encoded stable word
    # The fee was requested from the factory, not the pool.
    assert (_GET_FEE, _FACTORY_ADDR) in rpc.calls


def test_encode_solidly_get_fee_calldata():
    from almanak.connectors._strategy_base.solidly_pool_abi import encode_solidly_get_fee

    expected_pool_word = _POOL.lower().removeprefix("0x").zfill(64)
    assert encode_solidly_get_fee(_POOL, True) == _GET_FEE + expected_pool_word + "0" * 63 + "1"
    assert encode_solidly_get_fee(_POOL, False) == _GET_FEE + expected_pool_word + "0" * 64


@pytest.mark.parametrize("bad", ["0x1234", "not-an-address", "0x" + "z" * 40, "0x" + "a" * 41])
def test_encode_solidly_get_fee_rejects_malformed_address(bad):
    from almanak.connectors._strategy_base.solidly_pool_abi import encode_solidly_get_fee

    with pytest.raises(ValueError, match="20-byte hex address"):
        encode_solidly_get_fee(bad, True)


def test_plain_v2_pair_requires_positive_oracle_signal_and_reports_fee_unmeasured():
    rpc = _make_solidly_rpc_call(stable=None)
    reserves = _read(GatewayPoolReserveReader(rpc_call=rpc, token_resolver=_Resolver()))
    assert reserves.dex == "uniswap_v2"
    assert reserves.stable is None
    # V2 fees are not on-chain readable and fork fees differ — unmeasured, not guessed.
    assert reserves.fee_tier is None
    assert reserves.fee_percent is None
    assert reserves.reserve0 == Decimal("5000")
    # The classification consumed the positive V2 signal.
    assert any(sel == _PRICE0_CUMULATIVE for sel, _ in rpc.calls)


def test_solidly_detection_when_gateway_raises_on_slot0_revert():
    # Some gateway proxies surface a reverted eth_call as an exception rather
    # than empty bytes — detection must fall through to getReserves either way.
    reader = GatewayPoolReserveReader(
        rpc_call=_make_solidly_rpc_call(stable=False, slot0_behaviour=RuntimeError("execution reverted")),
        token_resolver=_Resolver(),
    )
    assert _read(reader).dex == "solidly_v2"


def test_unsupported_pool_shape_raises_data_unavailable():
    # Neither slot0() nor getReserves() answers -> loud typed failure.
    reader = GatewayPoolReserveReader(
        rpc_call=_make_solidly_rpc_call(overrides={_GET_RESERVES: "0x"}),
        token_resolver=_Resolver(),
    )
    with pytest.raises(DataUnavailableError, match="neither slot0"):
        _read(reader)


def test_get_reserves_of_two_words_is_unsupported_shape():
    # 64 bytes carries both reserve words but is non-canonical (both Solidly
    # and V2 ABI-encode 3 words = 96 bytes) — rejected, not half-decoded.
    reader = GatewayPoolReserveReader(
        rpc_call=_make_solidly_rpc_call(overrides={_GET_RESERVES: "0x" + _word(1) + _word(2)}),
        token_resolver=_Resolver(),
    )
    with pytest.raises(DataUnavailableError, match="neither slot0"):
        _read(reader)


def test_solidly_fee_read_failure_is_loud_never_guessed():
    reader = GatewayPoolReserveReader(
        rpc_call=_make_solidly_rpc_call(overrides={_GET_FEE: RuntimeError("factory boom")}),
        token_resolver=_Resolver(),
    )
    with pytest.raises(DataUnavailableError, match="chain read failed"):
        _read(reader)


def test_solidly_fee_empty_response_reports_factory_context():
    reader = GatewayPoolReserveReader(
        rpc_call=_make_solidly_rpc_call(overrides={_GET_FEE: "0x"}),
        token_resolver=_Resolver(),
    )
    with pytest.raises(DataUnavailableError, match=f"getFee.*factory {_FACTORY_ADDR}"):
        _read(reader)


def test_solidly_resolver_miss_falls_back_to_onchain_decimals():
    reserves = _read(
        GatewayPoolReserveReader(
            rpc_call=_make_solidly_rpc_call(stable=False),
            token_resolver=_Resolver(resolve_token1=False),
        )
    )
    assert reserves.token1.decimals == 18  # from the on-chain decimals() read
    assert reserves.reserve1 == Decimal("3")


def test_shape_detection_cached_across_reads():
    rpc = _make_solidly_rpc_call(stable=False)
    reader = GatewayPoolReserveReader(rpc_call=rpc, token_resolver=_Resolver())
    first = _read(reader)
    second = _read(reader)
    assert first.dex == second.dex == "solidly_v2"
    selectors = [sel for sel, _ in rpc.calls]
    # slot0() probed and stable() classified exactly once — the second read
    # hits the shape cache.
    assert selectors.count(_SLOT0) == 1
    assert selectors.count(_STABLE) == 1
    # Immutable facts (token0/token1, factory address) resolved once.
    assert selectors.count(_TOKEN0) == 1
    assert selectors.count(_TOKEN1) == 1
    assert selectors.count(_FACTORY) == 1
    # Mutable state re-read every time: reserves, and the fee (factories can
    # change it).
    assert selectors.count(_GET_RESERVES) == 2
    assert selectors.count(_GET_FEE) == 2


def test_solidly_tvl_computed_from_oracle():
    reader = GatewayPoolReserveReader(
        rpc_call=_make_solidly_rpc_call(stable=False),
        token_resolver=_Resolver(),
        price_oracle=_AsyncOracle({"USDC": Decimal("1"), "WETH": Decimal("3000")}),
    )
    # 5000 USDC * 1 + 3 WETH * 3000 = 14000
    assert _read(reader).tvl_usd == Decimal("14000")


# --------------------------------------------------------------------------- #
# Classification safety: transient failures must never poison the shape cache.
# The production gateway proxy maps transport failures AND reverts to the same
# empty bytes, so classification requires a definitive positive signal and the
# cache is written only after a fully successful read.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("behaviour", [RuntimeError("gateway blip"), "0x"], ids=["raises", "empty"])
def test_transient_stable_probe_failure_does_not_misclassify_solidly_pool(behaviour):
    # THE cache-poisoning regression: one flaky stable() call on a genuine
    # Solidly stable pool must NOT classify it as uniswap_v2 (6x wrong fee,
    # wrong price curve) — it must surface as transient and leave no cache.
    rpc = _flaky_once(_make_solidly_rpc_call(stable=True), _STABLE, behaviour)
    reader = GatewayPoolReserveReader(rpc_call=rpc, token_resolver=_Resolver())
    with pytest.raises(DataUnavailableError, match="cannot classify"):
        _read(reader)
    assert reader._pool_shape_cache == {}
    # Next read (probe healthy again) classifies correctly.
    reserves = _read(reader)
    assert reserves.dex == "solidly_v2"
    assert reserves.stable is True
    assert reserves.fee_tier == 500


def test_ambiguous_classification_never_cached_for_unknown_forks():
    # A pool answering getReserves() but neither stable() nor
    # price0CumulativeLast() cannot be classified safely -> transient error,
    # nothing cached, re-probed on the next read.
    rpc = _make_solidly_rpc_call(stable=None, overrides={_PRICE0_CUMULATIVE: "0x"})
    reader = GatewayPoolReserveReader(rpc_call=rpc, token_resolver=_Resolver())
    with pytest.raises(DataUnavailableError, match="cannot classify"):
        _read(reader)
    assert reader._pool_shape_cache == {}


def test_v3_shape_cached_only_after_full_read_succeeds():
    base = _make_rpc_call()
    rpc = _flaky_once(base, _LIQUIDITY, RuntimeError("gateway blip"))
    reader = GatewayPoolReserveReader(rpc_call=rpc, token_resolver=_Resolver())
    # First read: slot0 answers (shape looks V3) but liquidity() fails -> the
    # read fails transiently and the shape must NOT be cached yet.
    with pytest.raises(DataUnavailableError, match="chain read failed"):
        _read(reader)
    assert reader._pool_shape_cache == {}
    # Second read succeeds and caches.
    assert _read(reader).dex == "uniswap_v3"
    assert ("base", _POOL) in reader._pool_shape_cache


def test_solidly_mid_sequence_failure_leaves_cache_empty():
    rpc = _flaky_once(_make_solidly_rpc_call(stable=False), _TOKEN0, RuntimeError("gateway blip"))
    reader = GatewayPoolReserveReader(rpc_call=rpc, token_resolver=None)
    with pytest.raises(DataUnavailableError, match="chain read failed"):
        _read(reader)
    assert reader._pool_shape_cache == {}
    assert _read(reader).dex == "solidly_v2"


def test_cached_solidly_pool_transient_short_get_reserves_keeps_cache():
    rpc = _make_solidly_rpc_call(stable=False)
    reader = GatewayPoolReserveReader(rpc_call=rpc, token_resolver=_Resolver())
    assert _read(reader).dex == "solidly_v2"  # cached now
    flaky = _flaky_once(rpc, _GET_RESERVES, "0x")
    reader._rpc_call = flaky
    # A truncated response on a KNOWN pool is a transient error (not the
    # "neither shape" unsupported-pool message), and the cache survives.
    with pytest.raises(DataUnavailableError, match="response too short"):
        _read(reader)
    assert reader._pool_shape_cache[("base", _POOL)] == ("get_reserves", False)
    assert _read(reader).dex == "solidly_v2"
