"""VIB-5933: SlippageEstimator._is_zero_for_one must resolve SYMBOL inputs.

The pool contract stores ``token0`` as an ADDRESS. The old implementation
compared a bare symbol ("USDC", "WETH") to that address, never matched, and
fell through to ``return False`` — silently inverting swap direction whenever
``token_in`` was the pool's token0. A bare ``except: return True`` also
fabricated a direction on any RPC failure.

Deterministic repro from ``tests/reports/vib5922_oracle_gap_real_fork_proof.md``:
on the Optimism WETH/USDC pool USDC IS token0, so ``_is_zero_for_one("USDC", …)``
must be True. These tests pin both symbol orderings, both address orderings, and
the honest-failure paths (unresolvable token, RPC failure) that now raise
``DataUnavailableError`` instead of guessing.
"""

from __future__ import annotations

import pytest

from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.pools.liquidity import (
    LiquidityDepthReader,
    SlippageEstimator,
)
from almanak.framework.data.pools.reader import PoolReaderRegistry

# Optimism WETH/USDC pool token layout (from the VIB-5922 real-fork proof):
# USDC is token0, WETH is token1.
USDC_ADDR = "0x7f5c764cbc14f9669b88837ca1490cca17c31607"
WETH_ADDR = "0x4200000000000000000000000000000000000006"


def _token0_word(address: str) -> bytes:
    """ABI-encode an address as a 32-byte word (what token0() returns)."""
    return bytes(12) + bytes.fromhex(address[2:])


class _StubResolved:
    def __init__(self, address: str) -> None:
        self.address = address
        self.decimals = 18


class _SymbolResolver:
    """Resolves the two symbols to their addresses; raises on anything else."""

    _MAP = {"USDC": USDC_ADDR, "WETH": WETH_ADDR}

    def resolve_for_swap(self, token: str, chain: str) -> _StubResolved:  # noqa: ARG002
        try:
            return _StubResolved(self._MAP[token.upper()])
        except KeyError as exc:
            raise ValueError(f"unknown token {token}") from exc


def _estimator(*, rpc_call, token_resolver=None) -> SlippageEstimator:
    reader = LiquidityDepthReader(rpc_call=rpc_call)
    return SlippageEstimator(
        liquidity_reader=reader,
        pool_reader_registry=PoolReaderRegistry(rpc_call=lambda *a: b"\x00" * 32),
        token_resolver=token_resolver,
    )


def _est_with_token0(token0: str, *, token_resolver=None) -> SlippageEstimator:
    """Estimator whose pool token0() RPC read returns ``token0``."""
    return _estimator(rpc_call=lambda *a, **k: _token0_word(token0), token_resolver=token_resolver)


# ---------------------------------------------------------------------------
# Symbol inputs — the VIB-5933 bug
# ---------------------------------------------------------------------------


def test_symbol_token_in_is_token0_returns_true():
    """USDC (token0 symbol) -> WETH must be zeroForOne=True (was False pre-fix)."""
    est = _est_with_token0(USDC_ADDR, token_resolver=_SymbolResolver())
    assert est._is_zero_for_one("USDC", "WETH", "0xpool", "optimism", "uniswap_v3") is True


def test_symbol_token_in_is_token1_returns_false():
    """WETH (token1 symbol) -> USDC must be zeroForOne=False."""
    est = _est_with_token0(USDC_ADDR, token_resolver=_SymbolResolver())
    assert est._is_zero_for_one("WETH", "USDC", "0xpool", "optimism", "uniswap_v3") is False


# ---------------------------------------------------------------------------
# Address inputs — both directions (no resolver needed)
# ---------------------------------------------------------------------------


def test_address_token_in_is_token0_returns_true():
    est = _est_with_token0(USDC_ADDR)
    assert est._is_zero_for_one(USDC_ADDR, WETH_ADDR, "0xpool", "optimism", "uniswap_v3") is True


def test_address_token_in_is_token1_returns_false():
    est = _est_with_token0(USDC_ADDR)
    assert est._is_zero_for_one(WETH_ADDR, USDC_ADDR, "0xpool", "optimism", "uniswap_v3") is False


def test_address_checksummed_input_matches():
    """Checksummed address input still resolves to a case-insensitive match."""
    est = _est_with_token0(USDC_ADDR)
    checksummed = "0x7F5c764cBc14f9669B88837ca1490cCa17c31607"
    assert est._is_zero_for_one(checksummed, WETH_ADDR, "0xpool", "optimism", "uniswap_v3") is True


# ---------------------------------------------------------------------------
# Honest failure — no more fabricated direction
# ---------------------------------------------------------------------------


def test_unresolvable_symbol_raises_unavailable():
    """A symbol the resolver can't resolve -> neither side matches -> raise."""
    est = _est_with_token0(USDC_ADDR, token_resolver=_SymbolResolver())
    with pytest.raises(DataUnavailableError):
        est._is_zero_for_one("FOOBAR", "BAZ", "0xpool", "optimism", "uniswap_v3")


def test_unresolvable_token_out_raises_even_when_token_in_is_token0():
    """token_in resolves to token0 but token_out is unresolvable -> fail closed.

    A one-sided match must NOT fabricate a direction: without resolving
    token_out we cannot confirm the pair belongs to this pool, so proceeding
    into the tick-walk on ``in==token0`` would be a fabricated estimate.
    """
    est = _est_with_token0(USDC_ADDR, token_resolver=_SymbolResolver())
    with pytest.raises(DataUnavailableError):
        est._is_zero_for_one("USDC", "FOOBAR", "0xpool", "optimism", "uniswap_v3")


def test_unresolvable_token_in_raises_even_when_token_out_is_token0():
    """token_out resolves to token0 but token_in is unresolvable -> fail closed."""
    est = _est_with_token0(USDC_ADDR, token_resolver=_SymbolResolver())
    with pytest.raises(DataUnavailableError):
        est._is_zero_for_one("FOOBAR", "USDC", "0xpool", "optimism", "uniswap_v3")


def test_symbol_without_resolver_raises_unavailable():
    """Symbol input but no token resolver wired -> cannot resolve -> raise."""
    est = _est_with_token0(USDC_ADDR)  # no resolver
    with pytest.raises(DataUnavailableError):
        est._is_zero_for_one("USDC", "WETH", "0xpool", "optimism", "uniswap_v3")


def test_neither_token_is_token0_raises_unavailable():
    """Both tokens resolve but neither equals token0 (wrong pool) -> raise."""
    other = "0x1111111111111111111111111111111111111111"
    est = _est_with_token0(other)  # pool token0 is neither USDC nor WETH
    with pytest.raises(DataUnavailableError):
        est._is_zero_for_one(USDC_ADDR, WETH_ADDR, "0xpool", "optimism", "uniswap_v3")


def test_rpc_failure_raises_unavailable_not_fabricated_true():
    """token0() RPC read failing must raise, not fabricate zeroForOne=True."""

    def _boom(*a, **k):
        raise RuntimeError("rpc down")

    est = _estimator(rpc_call=_boom)
    with pytest.raises(DataUnavailableError):
        est._is_zero_for_one(USDC_ADDR, WETH_ADDR, "0xpool", "optimism", "uniswap_v3")
