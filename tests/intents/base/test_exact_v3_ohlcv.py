"""Real-fork, LIVE-upstream exact OHLCV observations for the Base V3 protocols.

The exact-pool OHLCV lane had **no live test at all** when it shipped: the only
coverage was a pinned replay UAT, whose recorded ``sourceUrl`` values encoded the
very ``before_timestamp`` the code was getting wrong, so it agreed with the bug and
was structurally incapable of catching it. The whole lane was consequently
dead-on-arrival — every request failed closed — and three review engines cleared
it (VIB-6734).

This module closes that gap. It exercises the real
``observe_exact_venue_data`` → gateway → CoinGecko Onchain path with the pool
binding verified against a real fork, and asserts the property the defect broke:
the returned candles cover **every** expected bucket of the half-open interval,
starting at ``start_at``.

Requires a CoinGecko key in the environment — either ``ALMANAK_GATEWAY_COINGECKO_API_KEY``
or the bare ``COINGECKO_API_KEY``, resolved through :func:`gateway_prefixed_or_bare`, the
same resolver the gateway itself uses (the gateway owns the egress — strategy-side code
never makes the call). Skipped when neither is set.
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.config.gateway_runtime import gateway_prefixed_or_bare
from almanak.connectors._strategy_base.v3_exact_data_provider import OHLCV_FEATURE_CONTRACT_VERSION
from almanak.connectors._strategy_base.v3_pool_validation import validate_v3_pool
from almanak.connectors._strategy_base.venue_verifier_registry import VenueVerifierRegistry
from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.framework.data.timeframes import OHLCVTimeframe
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.primitives.types import Primitive
from almanak.framework.venues import (
    ExactVenueFeatureRequest,
    ExactVenueObservation,
    OhlcvParameters,
    VenueBindingComponent,
    VenueReferenceNamespace,
    VenueTargetRef,
    VenueTargetRole,
    VenueVerificationRequest,
    VerifiedVenueBinding,
    observe_exact_venue_data,
)
from almanak.gateway.core.settings import GatewaySettings
from tests.conftest_gateway import AnvilFixture, GatewayServerThread, find_free_port
from tests.intents.conftest import CHAIN_CONFIGS

CHAIN_NAME = "base"

# How far back the requested window ends. Upstream needs the interval to be
# closed and indexed; anchoring two hours behind "now" keeps the request off the
# still-forming candle without pinning a date that would rot.
_SETTLE = timedelta(hours=2)

# The gateway's own wording when the returned candles miss an expected bucket.
# This exact string is the VIB-6734 signature and must remain a hard failure.
_COVERAGE_FAILURE = "did not cover the complete requested interval"

# Transient upstream conditions worth skipping for. Everything NOT named here is
# a hard failure: a live test that can skip on an unrecognised error is a live
# test that has quietly stopped being able to fail.
#
# The HTTP codes are matched with DIGIT BOUNDARIES, not as bare substrings. An
# earlier revision listed them as plain strings, which in a test whose entire
# domain is unix timestamps is a live collision rather than a theoretical one:
# "did not cover ... 1787150503" contains "503", and "malformed candle at index
# 429" contains "429". Both are genuine defects, and both would have skipped into
# a green run -- the exact failure the paragraph above warns against, rebuilt in
# the guard meant to prevent it.
_TRANSIENT_UPSTREAM_PHRASES = (
    "rate limit",
    "too many requests",
    "timed out",
    "timeout",
    "bad gateway",
    "service unavailable",
    "temporarily unavailable",
    "connection reset",
    "connection refused",
)
# A status code counts only when it is presented AS a status code. Digit
# boundaries alone are not enough -- "malformed candle at index 429" is
# digit-bounded and is a defect, not an outage -- so the code must be introduced
# by a token that genuinely presents a status.
#
# ONLY "http" and "status" qualify. An earlier revision also accepted "error" and
# "code", which re-opened the hole through the format this lane actually emits:
# venues/consumer.py builds detail as
#     f"exact venue data provider failed: {type(exc).__name__}: {exc}"
# and almost every Python exception name ends in "Error", so the token "error"
# lands two characters before the payload and the window reaches into it. The
# result was that the headline regression case skipped when routed through the
# real producer:
#     "...failed: IndexError: index 429 out of range"  -> matched "error: index 429"
# A bare "502 Bad Gateway" still skips via its phrase, and "http error 504" still
# skips via "http", so nothing real was lost by dropping the two tokens.
_TRANSIENT_HTTP_RE = re.compile(r"(?:http|status)\D{0,10}(?:429|502|503|504)(?!\d)")


def _is_transient_upstream(detail: str) -> bool:
    """True only for a NAMED transient upstream signal.

    Deliberately conservative in the direction that keeps the test able to fail:
    an unrecognised error is a hard failure, because a spurious failure is loud
    and gets investigated while a spurious skip is silent and reads as coverage.
    """
    lowered = detail.lower()
    if any(phrase in lowered for phrase in _TRANSIENT_UPSTREAM_PHRASES):
        return True
    return _TRANSIENT_HTTP_RE.search(lowered) is not None


@pytest.fixture(scope="module")
def ohlcv_gateway_client(anvil_base: AnvilFixture) -> Generator[GatewayClient, None, None]:
    """One insecure local gateway routed only to the managed Base fork.

    Deliberately a private copy of the equivalent fixture in
    ``test_exact_v3_twap.py`` rather than a shared conftest fixture: that module
    tests this fixture's own cleanup path by monkeypatching ``GatewayClient`` /
    ``GatewayServerThread`` in ITS module globals, so hoisting the fixture into a
    conftest would leave those patches pointing at the wrong namespace and the
    cleanup test would exercise the real classes while still passing.
    """
    server = None
    client = None
    try:
        port = find_free_port()
        server = GatewayServerThread(
            GatewaySettings(
                grpc_port=port,
                grpc_host="127.0.0.1",
                network="anvil",
                metrics_enabled=False,
                audit_enabled=False,
                allow_insecure=True,
                standalone=True,
            ),
            anvil_ports={"base": anvil_base.port},
        )
        server.start()
        client = GatewayClient(GatewayClientConfig(host="127.0.0.1", port=port))
        client.connect()
        assert client.health_check()
        yield client
    finally:
        primary_error = sys.exc_info()[1]
        cleanup_error: BaseException | None = None
        try:
            if client is not None:
                client.disconnect()
        except BaseException as exc:
            cleanup_error = exc
        finally:
            try:
                if server is not None:
                    server.stop()
            except BaseException as exc:
                if cleanup_error is None:
                    cleanup_error = exc
        if primary_error is None and cleanup_error is not None:
            raise cleanup_error


def _binding_block(web3: Web3) -> int:
    """The block the pool binding is verified against.

    The pinned ``ANVIL_FORK_BLOCK_BASE`` when the lane supplies one, else the
    fork's current head. Unlike TWAP, an OHLCV observation is anchored to the
    off-chain source rather than to a block, so the binding only needs a block
    the fork actually has — pinning is not load-bearing here, and requiring the
    env var would make this test skip-by-crash outside the configured lane.
    """
    pinned = os.environ.get("ANVIL_FORK_BLOCK_BASE")
    return int(pinned) if pinned else int(web3.eth.block_number)


def _verified_binding(protocol: str, fee_tier: int, web3: Web3, adapter) -> VerifiedVenueBinding:
    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    ordered = tuple(sorted((tokens["USDC"].lower(), tokens["WETH"].lower()), key=lambda item: int(item, 16)))
    pool = validate_v3_pool(
        CHAIN_NAME,
        protocol,
        ordered[0],
        ordered[1],
        fee_tier,
        web3.provider.endpoint_uri,  # type: ignore[attr-defined]
    )
    assert pool.exists is True and pool.pool_address

    request = VenueVerificationRequest(
        chain=CHAIN_NAME,
        protocol=protocol,
        primitive=Primitive.SWAP,
        requested_refs=(
            VenueTargetRef(
                role=VenueTargetRole.POOL,
                reference_namespace=VenueReferenceNamespace.EVM_ADDRESS,
                reference=pool.pool_address.lower(),
            ),
        ),
        ordered_assets=tuple(AssetIdentity(CHAIN_NAME, AssetNamespace.ERC20, address) for address in ordered),
        binding_components=(VenueBindingComponent("fee", str(fee_tier)),),
        binding_policy_version=1,
    )
    registry = VenueVerifierRegistry()
    verifier = registry.load_class(protocol)()
    verified = registry.validate_result(
        request,
        verifier.verify_venue(request, adapter, block_number=_binding_block(web3)),
    )
    assert type(verified) is VerifiedVenueBinding
    return verified


def _aligned_window(timeframe: OHLCVTimeframe, buckets: int) -> tuple[datetime, datetime]:
    """A half-open ``[start, end)`` interval of ``buckets`` closed candles."""
    step = timeframe.seconds
    end_ts = (int((datetime.now(UTC) - _SETTLE).timestamp()) // step) * step
    return (
        datetime.fromtimestamp(end_ts - buckets * step, tz=UTC),
        datetime.fromtimestamp(end_ts, tz=UTC),
    )


@pytest.mark.base
@pytest.mark.intent(IntentType.SWAP)  # noqa: layers -- data-only observation has no intent execution layers.
@pytest.mark.skipif(
    not gateway_prefixed_or_bare("COINGECKO_API_KEY"),
    reason=(
        "exact OHLCV reads a live upstream through the gateway; needs "
        "ALMANAK_GATEWAY_COINGECKO_API_KEY or COINGECKO_API_KEY"
    ),
)
@pytest.mark.parametrize(
    ("protocol", "fee_tier", "timeframe", "buckets"),
    (
        ("uniswap_v3", 3000, OHLCVTimeframe.ONE_HOUR, 4),
        ("uniswap_v3", 3000, OHLCVTimeframe.FIFTEEN_MINUTES, 6),
        ("pancakeswap_v3", 100, OHLCVTimeframe.ONE_HOUR, 3),
    ),
)
def test_verified_exact_v3_ohlcv_covers_every_expected_bucket(
    protocol: str,
    fee_tier: int,
    timeframe: OHLCVTimeframe,
    buckets: int,
    web3: Web3,
    anvil_eth_call_adapter,
    ohlcv_gateway_client: GatewayClient,
) -> None:
    """Every bucket of the half-open interval comes back, including the FIRST.

    This is the assertion VIB-6734 failed. ``before_timestamp`` is INCLUSIVE
    upstream, so passing the exclusive ``end_ts`` fetched the window one bucket
    late: the ``end_ts`` candle arrived and was discarded by the expected-set
    filter while ``start_at`` was never requested at all. The coverage check
    could then never pass and the observation failed closed every time — so a
    test that merely asserted "some candles" would still have caught nothing,
    but a test that asserts the FIRST bucket is present catches it exactly.
    """
    verified = _verified_binding(protocol, fee_tier, web3, anvil_eth_call_adapter)
    start_at, end_at = _aligned_window(timeframe, buckets)

    request = ExactVenueFeatureRequest(
        verified_binding=verified,
        parameters=OhlcvParameters(
            base_asset_index=0,
            quote_asset_index=1,
            timeframe=timeframe,
            start_at=start_at,
            end_at=end_at,
        ),
        feature_contract_version=OHLCV_FEATURE_CONTRACT_VERSION,
    )
    result = observe_exact_venue_data(request, ohlcv_gateway_client)

    if type(result) is not ExactVenueObservation:
        detail = str(getattr(result, "detail", ""))
        # The VIB-6734 signature is always a hard failure.
        if _COVERAGE_FAILURE in detail:
            pytest.fail(f"exact OHLCV did not cover the requested interval (VIB-6734): {detail}")
        # Skip on an ALLOWLIST of transient upstream signals, never on "everything
        # else". An earlier revision inverted this — it skipped unless the detail
        # matched the coverage signature — so an identity mismatch, a malformed
        # response or a gateway contract regression would have skipped silently.
        # The comment said "never widen this to any failure" while the code did
        # exactly that.
        if _is_transient_upstream(detail):
            pytest.skip(f"exact OHLCV upstream transiently unavailable: {detail}")
        pytest.fail(f"exact OHLCV failed for a NON-transient reason: {result}")
    candles = result.value
    assert len(candles) == buckets

    expected = tuple(int(start_at.timestamp()) + index * timeframe.seconds for index in range(buckets))
    assert tuple(int(candle.timestamp.timestamp()) for candle in candles) == expected

    # Identity echoes must survive the round trip, so the candles provably belong
    # to the binding that was verified against the fork.
    assert result.binding_hash == verified.binding.binding_hash
    assert result.feature_identity == request.feature_identity

    for candle in candles:
        assert type(candle.close) is Decimal and candle.close.is_finite() and candle.close > 0
        assert candle.low <= candle.high

    print(
        f"EXACT_OHLCV_OK protocol={protocol} tf={timeframe.value} buckets={buckets} "
        f"first={expected[0]} last={expected[-1]} binding={result.binding_hash}"
    )


@pytest.mark.base
@pytest.mark.intent(IntentType.SWAP)  # noqa: layers -- pure predicate guard, no intent execution layers.
@pytest.mark.parametrize(
    ("detail", "should_skip"),
    (
        # Genuine defects that MUST reach pytest.fail. Each of the first two skipped
        # under the original bare-substring allowlist, which is why this test exists.
        ("exact pool OHLCV response duplicated timestamp 1787150503", False),
        ("malformed candle at index 429 of the response", False),
        ("identity mismatch: expected binding 1787133604 got 1787137204", False),
        ("gateway contract regression: missing field", False),
        ("unexpected candle count 504 rows returned", False),
        # Real transient upstream signals that SHOULD skip.
        ("upstream returned HTTP 503 Service Unavailable", True),
        ("HTTP 429 Too Many Requests", True),
        ("http error 504", True),
        ("status: 502", True),
        ("502 Bad Gateway", True),  # no HTTP token, but its phrase carries it
        ("upstream request timed out after 30s", True),
        ("connection reset by peer", True),
        ("rate limit exceeded", True),
        # The REAL producer format: venues/consumer.py interpolates
        # type(exc).__name__, so "Error" precedes the payload on almost every
        # exception. These skipped while "error" was an accepted token.
        ("exact venue data provider failed: IndexError: index 429 out of range", False),
        ("exact venue data provider failed: ValueError: 503 candles returned, expected 4", False),
        ("exact venue data provider failed: KeyError: '504'", False),
        ("parse error at index 429", False),
        # ...while genuine transients through the same wrapper still skip.
        ("exact venue data provider failed: DataSourceTimeout: upstream slow", True),
        ("exact venue data provider failed: ClientResponseError: HTTP 503: upstream", True),
    ),
)
def test_transient_allowlist_skips_only_named_upstream_signals(detail: str, should_skip: bool) -> None:
    """The skip allowlist must not swallow a defect that merely contains a number.

    A live test that skips on an unrecognised error has quietly stopped being able
    to fail, and a spurious skip is silent where a spurious failure is loud. The
    first two cases are regressions: under the original bare-substring list,
    ``"...1787150503"`` matched ``"503"`` and ``"index 429"`` matched ``"429"``, so
    a duplicated-timestamp bug and a malformed-response bug both skipped green.
    """
    assert _is_transient_upstream(detail) is should_skip
