"""Hermetic unit tests for the Curve pair-resolver payload adapter (ALM-3365)."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.connectors.curve.pair_resolver import PairCandidate, PairCandidateSet, resolve_pair_payload

POOL = "0x11c1fbd4b3de66bc0565779b35171a6cf3e71f59"
LP_TOKEN = "0x98244d93d42b42ab3e3a4d12a5dc0b3e7f8f32f9"


def _candidate_set(*, ranked, rejected=(), indeterminate=False):
    return PairCandidateSet(
        chain="base", pair_label="cbETH/WETH", ranked=list(ranked), rejected=list(rejected), indeterminate=indeterminate
    )


def _candidate(metadata=None):
    return PairCandidate(
        address=POOL,
        metadata=metadata,
        liquidity_usd=Decimal("1360000"),
        provenance_suspect=False,
        rejection=None,
    )


def test_indeterminate_transport_raises_not_empty():
    with patch(
        "almanak.connectors.curve.pair_resolver.build_pair_candidates",
        return_value=_candidate_set(ranked=[], indeterminate=True),
    ):
        with pytest.raises(RuntimeError, match="indeterminate"):
            resolve_pair_payload("base", "0xa", "0xb")


def test_empty_ranked_is_an_honest_miss():
    with patch(
        "almanak.connectors.curve.pair_resolver.build_pair_candidates",
        return_value=_candidate_set(ranked=[]),
    ):
        assert resolve_pair_payload("base", "0xa", "0xb") is None


def test_ranked_candidate_serializes_metadata():
    meta = SimpleNamespace(
        lp_token=LP_TOKEN,
        coin_addresses=("0xa", "0xb"),
        coin_symbols=("cbETH", "WETH"),
        pool_type="cryptoswap",
        is_metapool=False,
    )
    rejected = [
        PairCandidate(address="0xdead", metadata=None, liquidity_usd=None, provenance_suspect=True, rejection="dust")
    ]
    with patch(
        "almanak.connectors.curve.pair_resolver.build_pair_candidates",
        return_value=_candidate_set(ranked=[_candidate(meta)], rejected=rejected),
    ):
        payload = resolve_pair_payload("base", "0xa", "0xb")
    assert payload["pool_address"] == POOL
    assert payload["lp_token"] == LP_TOKEN
    assert payload["pool_type"] == "cryptoswap"
    assert payload["coins"] == ["0xa", "0xb"]
    assert payload["candidates_rejected"] == 1
    assert payload["resolved_via"] == "meta_registry_find_pool_for_coins"


def test_metadata_less_candidate_still_resolves():
    with patch(
        "almanak.connectors.curve.pair_resolver.build_pair_candidates",
        return_value=_candidate_set(ranked=[_candidate()]),
    ):
        payload = resolve_pair_payload("base", "0xa", "0xb")
    assert payload["pool_address"] == POOL
    assert "lp_token" not in payload
