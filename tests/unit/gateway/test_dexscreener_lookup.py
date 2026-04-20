"""Unit tests for the DexScreener symbol-to-address lookup (VIB-2983).

Covers:
- The 4-gate policy (liquidity / volume / turnover / dominance)
- LUME real-world snapshot regression (all Base candidates rejected)
- Case-insensitive symbol matching
- Chain-slug mapping coverage for every SDK-supported chain
- HTTP error paths (429 retry + eventual failure, non-200 failure, malformed payload)
- Metrics counters
- Candidate extraction from both baseToken and quoteToken sides

All tests mock aiohttp via ``unittest.mock.patch`` on the module-level
``aiohttp`` import inside ``_fetch_pairs`` — no live network calls.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import patch

import pytest

from almanak.framework.data.tokens.exceptions import AmbiguousTokenError
from almanak.gateway.services import dexscreener_lookup
from almanak.gateway.services.dexscreener_lookup import (
    CHAIN_SLUG_MAP,
    DexScreenerError,
    _apply_gates,
    _Candidate,
    _extract_candidates_on_chain,
    chain_slug_for,
    find_token_address,
    get_metrics_snapshot,
)


# =============================================================================
# Fake aiohttp session -- lets us control status + payload without real I/O
# =============================================================================


class _FakeResponse:
    def __init__(self, status: int, payload: Any) -> None:
        self.status = status
        self._payload = payload

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        return None

    async def json(self, content_type: str | None = None) -> Any:
        return self._payload


class _FakeSession:
    """An aiohttp.ClientSession test double.

    ``responses`` is a list of (status, payload) tuples yielded in order
    across ``get()`` calls, so a test can simulate 429-then-200.
    """

    def __init__(self, responses: list[tuple[int, Any]]) -> None:
        self._responses = list(responses)
        self.get_calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any] | None = None, timeout: Any = None) -> _FakeResponse:
        self.get_calls.append((url, dict(params or {})))
        if not self._responses:
            raise AssertionError("FakeSession exhausted: got more GETs than responses")
        status, payload = self._responses.pop(0)
        return _FakeResponse(status, payload)

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        return None


def _pair(
    chain: str,
    base_symbol: str,
    base_address: str,
    *,
    quote_symbol: str = "WETH",
    quote_address: str = "0x4200000000000000000000000000000000000006",
    liquidity_usd: float = 1_000_000,
    volume_24h_usd: float = 100_000,
    url: str | None = None,
) -> dict[str, Any]:
    return {
        "chainId": chain,
        "baseToken": {"symbol": base_symbol, "address": base_address},
        "quoteToken": {"symbol": quote_symbol, "address": quote_address},
        "liquidity": {"usd": liquidity_usd},
        "volume": {"h24": volume_24h_usd},
        "url": url,
    }


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    dexscreener_lookup._reset_for_tests()
    yield
    dexscreener_lookup._reset_for_tests()


# =============================================================================
# Chain-slug mapping
# =============================================================================


class TestChainSlug:
    def test_maps_known_chains_case_insensitively(self) -> None:
        assert chain_slug_for("Arbitrum") == "arbitrum"
        assert chain_slug_for("BASE") == "base"

    def test_returns_none_for_unknown_chain(self) -> None:
        assert chain_slug_for("fantom_opera") is None

    @pytest.mark.parametrize(
        "sdk_chain",
        [
            "ethereum",
            "arbitrum",
            "optimism",
            "base",
            "polygon",
            "avalanche",
            "bsc",
            "sonic",
            "mantle",
            "berachain",
            "monad",
            "xlayer",
            "zerog",
            "blast",
            "linea",
            "plasma",
        ],
    )
    def test_every_sdk_chain_mapped(self, sdk_chain: str) -> None:
        """All chains we care about for EVM symbol resolution must have a mapping."""
        assert sdk_chain in CHAIN_SLUG_MAP

    def test_map_is_subset_of_allowed_chains(self) -> None:
        """Every key in CHAIN_SLUG_MAP must be in ``validation.ALLOWED_CHAINS``.

        A chain listed here that the gateway validation layer rejects upstream
        is dead code. Solana is exempt because its resolution path goes
        through Jupiter (not this module).
        """
        from almanak.gateway.validation import ALLOWED_CHAINS

        for sdk_chain in CHAIN_SLUG_MAP:
            if sdk_chain == "solana":
                continue
            assert sdk_chain in ALLOWED_CHAINS, (
                f"{sdk_chain} is in CHAIN_SLUG_MAP but not in validation.ALLOWED_CHAINS"
            )


# =============================================================================
# Candidate extraction
# =============================================================================


class TestExtractCandidates:
    def test_matches_symbol_on_base_side(self) -> None:
        pairs = [_pair("base", "LUME", "0xAAA")]
        candidates = _extract_candidates_on_chain(pairs, symbol="LUME", chain_slug="base")
        assert len(candidates) == 1
        assert candidates[0].address == "0xAAA"

    def test_matches_symbol_on_quote_side(self) -> None:
        pairs = [
            _pair(
                "base",
                base_symbol="WETH",
                base_address="0xWETH",
                quote_symbol="LUME",
                quote_address="0xBBB",
            ),
        ]
        candidates = _extract_candidates_on_chain(pairs, symbol="LUME", chain_slug="base")
        assert [c.address for c in candidates] == ["0xBBB"]

    def test_filters_wrong_chain(self) -> None:
        pairs = [
            _pair("base", "TKN", "0xBASE"),
            _pair("ethereum", "TKN", "0xETH"),
        ]
        candidates = _extract_candidates_on_chain(pairs, symbol="TKN", chain_slug="base")
        assert [c.address for c in candidates] == ["0xBASE"]

    def test_is_case_insensitive(self) -> None:
        pairs = [_pair("base", "lume", "0xCCC")]
        candidates = _extract_candidates_on_chain(pairs, symbol="LUME", chain_slug="BASE")
        assert len(candidates) == 1

    def test_skips_pairs_without_address(self) -> None:
        pairs = [
            {
                "chainId": "base",
                "baseToken": {"symbol": "LUME", "address": ""},
                "quoteToken": {"symbol": "WETH", "address": "0xWETH"},
                "liquidity": {"usd": 100_000},
                "volume": {"h24": 10_000},
            }
        ]
        candidates = _extract_candidates_on_chain(pairs, symbol="LUME", chain_slug="base")
        assert candidates == []


# =============================================================================
# Gates 1-3: liquidity / volume / turnover
# =============================================================================


class TestLiquidityGate:
    @pytest.mark.parametrize(
        "liq_usd,expected",
        [
            (9_999.0, False),
            (10_000.0, True),
            (50_000.0, True),
        ],
    )
    def test_boundary(self, liq_usd: float, expected: bool) -> None:
        candidate = _Candidate(
            address="0xAAA",
            symbol="TKN",
            chain_slug="base",
            liquidity_usd=liq_usd,
            volume_24h_usd=50_000.0,
            pair_url=None,
        )
        result = _apply_gates([candidate], symbol="TKN", chain="base")
        assert (result is not None) is expected


class TestVolumeGate:
    @pytest.mark.parametrize(
        "vol_usd,expected",
        [
            (500.0, False),
            (999.0, False),
            (1_000.0, True),
            (5_000.0, True),
        ],
    )
    def test_boundary(self, vol_usd: float, expected: bool) -> None:
        # Liquidity sized so gate-3 turnover always passes (vol/liq >= 5%);
        # only gate-2 volume floor determines the outcome.
        candidate = _Candidate(
            address="0xAAA",
            symbol="TKN",
            chain_slug="base",
            liquidity_usd=10_000.0,
            volume_24h_usd=vol_usd,
            pair_url=None,
        )
        result = _apply_gates([candidate], symbol="TKN", chain="base")
        assert (result is not None) is expected


class TestTurnoverGate:
    def test_low_turnover_rejected(self) -> None:
        # $50k liq, $1k volume -> 2% turnover, below 5% threshold
        candidate = _Candidate(
            address="0xAAA",
            symbol="TKN",
            chain_slug="base",
            liquidity_usd=50_000.0,
            volume_24h_usd=1_000.0,
            pair_url=None,
        )
        assert _apply_gates([candidate], symbol="TKN", chain="base") is None

    def test_healthy_turnover_accepted(self) -> None:
        # $50k liq, $3k volume -> 6% turnover, above 5% threshold
        candidate = _Candidate(
            address="0xAAA",
            symbol="TKN",
            chain_slug="base",
            liquidity_usd=50_000.0,
            volume_24h_usd=3_000.0,
            pair_url=None,
        )
        result = _apply_gates([candidate], symbol="TKN", chain="base")
        assert result is not None
        assert result.address == "0xAAA"


# =============================================================================
# Gate 4: dominance
# =============================================================================


class TestDominanceGate:
    def test_dominant_leader_accepted(self) -> None:
        # $50k vs $10k -> 5x, above 3x threshold
        candidates = [
            _Candidate("0xWIN", "TKN", "base", 50_000.0, 5_000.0, None),
            _Candidate("0xLOSE", "TKN", "base", 10_000.0, 2_000.0, None),
        ]
        result = _apply_gates(candidates, symbol="TKN", chain="base")
        assert result is not None
        assert result.address == "0xWIN"

    def test_non_dominant_raises_ambiguous(self) -> None:
        # $50k vs $40k -> 1.25x, below 3x threshold
        candidates = [
            _Candidate("0xAAA", "TKN", "base", 50_000.0, 5_000.0, None),
            _Candidate("0xBBB", "TKN", "base", 40_000.0, 4_000.0, None),
        ]
        with pytest.raises(AmbiguousTokenError) as exc_info:
            _apply_gates(candidates, symbol="TKN", chain="base")
        err = exc_info.value
        assert "0xAAA" in err.matching_addresses
        assert "0xBBB" in err.matching_addresses
        # Every candidate included in the suggestions so the user can pick
        joined = "\n".join(err.suggestions)
        assert "0xAAA" in joined and "0xBBB" in joined

    def test_single_candidate_never_raises(self) -> None:
        candidates = [_Candidate("0xONLY", "TKN", "base", 20_000.0, 2_000.0, None)]
        result = _apply_gates(candidates, symbol="TKN", chain="base")
        assert result is not None and result.address == "0xONLY"


# =============================================================================
# Address aggregation (same contract appearing in multiple pairs)
# =============================================================================


class TestAddressAggregation:
    def test_same_address_multiple_pairs_picks_highest_liquidity_pair(self) -> None:
        # Same token in two pools. We pick the SINGLE best pool and use
        # its own (liq, vol) together — not the synthetic max across pools.
        # Here pool B (15k liq / 2k vol / 13% turnover) passes all gates.
        candidates = [
            _Candidate("0xSAME", "TKN", "base", 6_000.0, 500.0, None),
            _Candidate("0xSAME", "TKN", "base", 15_000.0, 2_000.0, None),
        ]
        result = _apply_gates(candidates, symbol="TKN", chain="base")
        assert result is not None
        assert result.liquidity_usd == 15_000.0
        assert result.volume_24h_usd == 2_000.0

    def test_synthetic_candidate_is_not_constructed(self) -> None:
        # Codex P1 regression: previously this would merge pool A's liquidity
        # ($50k) with pool B's volume ($3k) into a fake 6%-turnover candidate.
        # Pool A on its own is 2% turnover (fails), pool B is 6% turnover but
        # fails gate 1 (liquidity below $10k). Neither individual pool passes
        # all three gates, so the result must be None.
        candidates = [
            _Candidate("0xSAME", "TKN", "base", 50_000.0, 1_000.0, None),  # 2% turnover -> fail gate 3
            _Candidate("0xSAME", "TKN", "base", 5_000.0, 3_000.0, None),   # $5k liq -> fail gate 1
        ]
        result = _apply_gates(candidates, symbol="TKN", chain="base")
        assert result is None, "Aggregation must not synthesize a healthy candidate from unhealthy pools"


# =============================================================================
# LUME real-world snapshot regression
# =============================================================================


class TestLumeRegression:
    """Golden regression for the motivating smoke-test (2026-04-17).

    Verifies that the real DexScreener payload for ``?q=LUME`` is rejected
    by the gating policy. If this test ever starts returning a result, the
    gates have been weakened.
    """

    LUME_PAIRS = [
        _pair(
            "base",
            "LUME",
            "0x903fb71e53C9CCe1717E0d74A473d49C48201B07",
            liquidity_usd=25_561.53,
            volume_24h_usd=9.94,
        ),
        _pair(
            "base",
            "LUME",
            "0x9D7AfDB981EA520F45774671a9104Fc99aF9e519",
            liquidity_usd=11_287.85,
            volume_24h_usd=57.82,
        ),
        _pair(
            "base",
            "LUME",
            "0xb2B4EADcB2077Bc18423A1E4fB9aA850b6120FBb",
            liquidity_usd=10_006.44,
            volume_24h_usd=59.95,
        ),
        _pair(
            "solana",
            "Lume",
            "BmjkLfs7ETBSrKbwiceKXxPPwNBqGJkVN2mwyxajpump",
            liquidity_usd=3_111.0,
            volume_24h_usd=1.0,
        ),
    ]

    @pytest.mark.asyncio
    async def test_lume_rejected_on_base(self) -> None:
        fake = _FakeSession([(200, {"pairs": self.LUME_PAIRS})])
        result = await find_token_address("LUME", "base", session=fake)
        assert result is None

    @pytest.mark.asyncio
    async def test_lume_rejected_on_solana(self) -> None:
        fake = _FakeSession([(200, {"pairs": self.LUME_PAIRS})])
        result = await find_token_address("LUME", "solana", session=fake)
        assert result is None


# =============================================================================
# HTTP behaviour
# =============================================================================


class TestHttpBehaviour:
    @pytest.mark.asyncio
    async def test_200_happy_path(self) -> None:
        fake = _FakeSession(
            [
                (
                    200,
                    {
                        "pairs": [
                            _pair(
                                "base",
                                "NEW",
                                "0xNEW",
                                liquidity_usd=50_000.0,
                                volume_24h_usd=5_000.0,
                            ),
                        ]
                    },
                )
            ]
        )
        result = await find_token_address("NEW", "base", session=fake)
        assert result is not None
        assert result.address == "0xNEW"
        snap = get_metrics_snapshot()
        assert snap["resolved_total"] == 1

    @pytest.mark.asyncio
    async def test_429_then_200_retries_once(self) -> None:
        # Avoid real sleep
        with patch.object(dexscreener_lookup.asyncio, "sleep", new=_no_sleep):
            fake = _FakeSession(
                [
                    (429, {}),
                    (
                        200,
                        {
                            "pairs": [
                                _pair(
                                    "base",
                                    "NEW",
                                    "0xNEW",
                                    liquidity_usd=50_000.0,
                                    volume_24h_usd=5_000.0,
                                ),
                            ]
                        },
                    ),
                ]
            )
            result = await find_token_address("NEW", "base", session=fake)
        assert result is not None
        assert len(fake.get_calls) == 2

    @pytest.mark.asyncio
    async def test_persistent_429_raises(self) -> None:
        with patch.object(dexscreener_lookup.asyncio, "sleep", new=_no_sleep):
            fake = _FakeSession([(429, {}), (429, {})])
            with pytest.raises(DexScreenerError):
                await find_token_address("NEW", "base", session=fake)

    @pytest.mark.asyncio
    async def test_non_200_raises(self) -> None:
        fake = _FakeSession([(500, {})])
        with pytest.raises(DexScreenerError):
            await find_token_address("NEW", "base", session=fake)

    @pytest.mark.asyncio
    async def test_malformed_payload_raises(self) -> None:
        fake = _FakeSession([(200, "not a dict")])
        with pytest.raises(DexScreenerError):
            await find_token_address("NEW", "base", session=fake)

    @pytest.mark.asyncio
    async def test_pairs_missing_is_empty(self) -> None:
        fake = _FakeSession([(200, {})])  # no "pairs" key
        result = await find_token_address("NEW", "base", session=fake)
        assert result is None


# =============================================================================
# Unmapped chain
# =============================================================================


class TestUnmappedChain:
    @pytest.mark.asyncio
    async def test_returns_none_for_unmapped_chain(self) -> None:
        # Fantom is not in CHAIN_SLUG_MAP, so no HTTP call should be made
        fake = _FakeSession([])  # would raise AssertionError if get() called
        result = await find_token_address("TKN", "fantom_opera", session=fake)
        assert result is None
        assert fake.get_calls == []


# =============================================================================
# Helpers
# =============================================================================


async def _no_sleep(_delay: float) -> None:
    """Replacement for ``asyncio.sleep`` that returns immediately."""
    return None
