"""Hermetic unit tests for the V4 pair-resolver payload adapter (ALM-3365)."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.connectors.uniswap_v4.pair_resolver import resolve_pair_payload

POOL_ID_500 = "0x" + "05" * 32
POOL_ID_3000 = "0x" + "30" * 32


class _FakeReader:
    def __init__(self, pools: dict[int, tuple[str, int]], unreadable: set[str] | None = None, **_kwargs):
        self._pools = pools
        self._unreadable = unreadable or set()

    def resolve_pool_address(self, token_a, token_b, chain, fee_tier):
        entry = self._pools.get(fee_tier)
        return entry[0] if entry else None

    def read_pool_price(self, pool_id, chain):
        if pool_id in self._unreadable:
            raise RuntimeError("unreadable")
        liquidity = next(liq for pid, liq in self._pools.values() if pid == pool_id)
        return SimpleNamespace(
            value=SimpleNamespace(
                price=Decimal("2000"), tick=100, liquidity=liquidity, token0_decimals=18, token1_decimals=6
            )
        )


def _patched(pools, unreadable=None):
    def _factory(**kwargs):
        return _FakeReader(pools, unreadable, **kwargs)

    return patch("almanak.framework.data.pools.reader.UniswapV4PoolReader", side_effect=_factory)


def test_explicit_tier_resolves_that_pool():
    with _patched({500: (POOL_ID_500, 10), 3000: (POOL_ID_3000, 99)}):
        payload = resolve_pair_payload("base", "0xa", "0xb", fee_tier=500)
    assert payload["pool_address"] == POOL_ID_500
    assert payload["fee_tier"] == 500
    assert payload["fee_tier_source"] == "explicit"
    assert payload["tick"] == 100


def test_sweep_picks_deepest_tier():
    with _patched({500: (POOL_ID_500, 10), 3000: (POOL_ID_3000, 99)}):
        payload = resolve_pair_payload("base", "0xa", "0xb")
    assert payload["pool_address"] == POOL_ID_3000
    assert payload["fee_tier_source"] == "sweep"


def test_no_initialized_pool_returns_none():
    with _patched({}):
        assert resolve_pair_payload("base", "0xa", "0xb") is None


def test_unreadable_candidate_is_skipped_not_fatal():
    with _patched({500: (POOL_ID_500, 10), 3000: (POOL_ID_3000, 99)}, unreadable={POOL_ID_3000}):
        payload = resolve_pair_payload("base", "0xa", "0xb")
    assert payload["pool_address"] == POOL_ID_500


def test_all_reads_faulting_raises_indeterminate():
    with _patched({500: (POOL_ID_500, 10)}, unreadable={POOL_ID_500}):
        with pytest.raises(RuntimeError, match="indeterminate"):
            resolve_pair_payload("base", "0xa", "0xb")
