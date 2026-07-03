"""ResultEnricher threads ``pool_meta_lookup`` only to declaring connectors (VIB-5628).

Mirrors the ``pool_key_lookup`` carve-out discipline: the enricher sends the
Curve dynamic-pool-meta lookup callable only to receipt parsers whose connector
declares ``pool_meta_lookup`` in its manifest ``receipt_parser_kwargs`` — never
to other parsers (which would bust their protocol cache).
"""

from __future__ import annotations

from almanak.framework.execution.result_enricher import (
    ResultEnricher,
    _pool_key_lookup_protocols,
    _pool_meta_lookup_protocols,
)


def test_curve_declares_pool_meta_lookup() -> None:
    keys = _pool_meta_lookup_protocols()
    assert "curve" in keys
    # V4 declares pool_key_lookup, NOT pool_meta_lookup — the two carve-outs are disjoint.
    assert "uniswap_v4" not in keys


def test_v4_declares_pool_key_not_pool_meta() -> None:
    assert "uniswap_v4" in _pool_key_lookup_protocols()
    assert "curve" not in _pool_key_lookup_protocols()


def test_kwarg_threaded_only_to_curve() -> None:
    sentinel = object()
    enricher = ResultEnricher(pool_meta_lookup=sentinel)

    curve_kwargs = enricher._build_parser_kwargs("curve", "ethereum")
    assert curve_kwargs.get("pool_meta_lookup") is sentinel

    # A non-declaring parser must NOT receive the kwarg.
    other_kwargs = enricher._build_parser_kwargs("uniswap_v3", "ethereum")
    assert "pool_meta_lookup" not in other_kwargs


def test_kwarg_absent_when_lookup_none() -> None:
    enricher = ResultEnricher()  # pool_meta_lookup defaults to None
    curve_kwargs = enricher._build_parser_kwargs("curve", "ethereum")
    assert "pool_meta_lookup" not in curve_kwargs
