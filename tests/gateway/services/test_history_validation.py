"""D3.F8 validator tests for PoolHistoryService (VIB-4751 / POOL-3).

Maps to the umbrella UAT card at ``docs/internal/uat-cards/VIB-4728.md``:
- D3.F8 (validator-level rejections — table of bad inputs and expected codes)
- D3.F6 (silent-error guard's validator-level fast path: empty address /
  traversal protocol rejected BEFORE any provider is consulted)
- Inherited audit rows #3 / #5 / #13 (chain-aware normalize, canonical-
  segment equality, strip-then-lowercase EVM)

The validator is in ``almanak/gateway/services/pool_history_service.py``;
the shared address/normalize/cap helpers are in
``almanak/gateway/services/_history_common.py``. Both surfaces are tested
here so a regression to either fails this file.
"""

from __future__ import annotations

import asyncio

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services._history_common import (
    END_TS_FUTURE_TOLERANCE_SECONDS,
    SUPPORTED_NORMALIZATION_CHAINS,
    get_soft_cap_seconds,
    is_solana_chain,
    normalize_pool_address,
    resolution_to_seconds,
    validate_pool_address_syntax,
)
from almanak.gateway.services.pool_history_service import (
    POOL_PROTOCOL_ALLOWLIST,
    SUPPORTED_POOL_PAIRS,
    PoolHistoryServiceServicer,
    _validate_pool_history_request,
    is_supported_pool_pair,
)


# Known-good EVM pool (Arbitrum USDC/WETH 0.05% UniV3).
_VALID_EVM_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"
# Known-good Solana pool (Raydium USDC/SOL) — case preserved.
_VALID_SOLANA_POOL = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2"

_NOW = 1_750_000_000  # well after any valid start_ts/end_ts in these tests
_VALID_START = 1_700_000_000
_VALID_END = 1_700_604_800  # +7d, well in the past relative to _NOW


def _make_request(**overrides) -> gateway_pb2.PoolHistoryRequest:
    """Build a syntactically-valid request; override one field per test."""
    fields = {
        "pool_address": _VALID_EVM_POOL,
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "start_ts": _VALID_START,
        "end_ts": _VALID_END,
        "resolution": gateway_pb2.Resolution.RESOLUTION_1H,
    }
    fields.update(overrides)
    return gateway_pb2.PoolHistoryRequest(**fields)


# ============================================================================
# Validator function: positive-path baseline
# ============================================================================


def test_validator_accepts_well_formed_request():
    """The baseline _make_request() MUST validate clean — every negative
    test below changes one field, so the baseline being green is the
    invariant the table tests rely on."""
    assert _validate_pool_history_request(_make_request(), now_seconds=_NOW) is None


def test_validator_accepts_above_soft_cap_request():
    """UAT card D3.F8: 91d at RESOLUTION_1H (above default 90d soft cap)
    MUST be ACCEPTED at the validator. Soft cap is a handler-side
    decision (POOL-6 truncates to CAP_EXCEEDED); the validator does not
    enforce it."""
    long_end = _VALID_START + 91 * 86400  # 91 days
    # Move "now" forward to make end_ts not-in-the-future.
    fresh_now = long_end + 1000
    request = _make_request(end_ts=long_end)
    assert _validate_pool_history_request(request, now_seconds=fresh_now) is None


# ============================================================================
# Validator function: D3.F8 table (parametrized over each bad row)
# ============================================================================


@pytest.mark.parametrize(
    "field, value, expected_substr",
    [
        # Resolution
        ("resolution", gateway_pb2.Resolution.RESOLUTION_UNSPECIFIED, "resolution"),
        # Chain
        ("chain", "", "chain is required"),
        ("chain", "   ", "chain is required"),
        # Protocol (allowlist) — chain is arbitrum, protocol="uniswap_v999" is unknown
        ("protocol", "", "protocol is required"),
        ("protocol", "uniswap_v999", "unsupported protocol"),
        # Path-traversal injection guard
        ("protocol", "../etc/passwd", "unsupported protocol"),
        # Pool address
        ("pool_address", "", "pool_address is required"),
        ("pool_address", "0x", "invalid pool_address"),
        ("pool_address", "0xZZZ", "invalid pool_address"),
        ("pool_address", "0x" + "z" * 40, "invalid pool_address"),  # non-hex EVM
        # Timestamps
        ("start_ts", 0, "start_ts must be > 0"),
        ("end_ts", 0, "end_ts must be > 0"),
        ("start_ts", -1, "start_ts must be > 0"),
        ("end_ts", -1, "end_ts must be > 0"),
    ],
)
def test_validator_rejects_invalid_field(field: str, value, expected_substr: str):
    request = _make_request(**{field: value})
    failure = _validate_pool_history_request(request, now_seconds=_NOW)
    assert failure is not None, f"expected INVALID_ARGUMENT for {field}={value!r}"
    code, message = failure
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    assert expected_substr.lower() in message.lower(), (
        f"expected message to contain {expected_substr!r}, got {message!r}"
    )


def test_validator_rejects_start_after_end():
    request = _make_request(start_ts=_VALID_END + 1, end_ts=_VALID_END)
    failure = _validate_pool_history_request(request, now_seconds=_NOW)
    assert failure is not None
    code, message = failure
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    assert "must be <" in message


def test_validator_rejects_start_equals_end():
    """An empty time window is not "no data" — it's an invalid request.
    Forwarding it would burn a provider quota for zero bars."""
    request = _make_request(start_ts=_VALID_END, end_ts=_VALID_END)
    failure = _validate_pool_history_request(request, now_seconds=_NOW)
    assert failure is not None
    assert failure[0] == grpc.StatusCode.INVALID_ARGUMENT


def test_validator_rejects_future_end_ts_beyond_tolerance():
    """end_ts must not be too far in the future (defense against clock-skew
    + accidental ``now() + months``). The tolerance is 5 minutes; this
    test pushes 1 hour beyond ``now``."""
    request = _make_request(end_ts=_NOW + 3600)
    failure = _validate_pool_history_request(request, now_seconds=_NOW)
    assert failure is not None
    code, message = failure
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    assert "future" in message.lower()


def test_validator_accepts_end_ts_within_skew_tolerance():
    """end_ts up to 5 minutes (END_TS_FUTURE_TOLERANCE_SECONDS) in the
    future is accepted to absorb NTP skew."""
    request = _make_request(end_ts=_NOW + END_TS_FUTURE_TOLERANCE_SECONDS)
    assert _validate_pool_history_request(request, now_seconds=_NOW) is None


# ============================================================================
# (chain, protocol) compatibility (Codex Round-8 fix #2)
# ============================================================================


@pytest.mark.parametrize(
    "chain, protocol",
    [
        ("ethereum", "aerodrome"),  # Aerodrome not deployed on Ethereum
        ("arbitrum", "aerodrome"),  # Aerodrome not deployed on Arbitrum
        ("optimism", "aerodrome"),  # only Base for Aerodrome
        ("polygon", "aerodrome"),  # only Base for Aerodrome
        ("solana", "uniswap_v3"),  # Solana out of scope for VIB-4728
        ("solana", "aerodrome"),
    ],
)
def test_validator_rejects_unsupported_chain_protocol_pairs(chain: str, protocol: str):
    """UAT card D3.F8: chain × protocol compatibility row. Aerodrome only
    on Base; Solana is out of scope for VIB-4728."""
    # The pool_address has to syntactically match the chain (Solana base58
    # vs EVM hex) or we'd hit the address-syntax check first.
    address = _VALID_SOLANA_POOL if chain == "solana" else _VALID_EVM_POOL
    request = _make_request(chain=chain, protocol=protocol, pool_address=address)
    failure = _validate_pool_history_request(request, now_seconds=_NOW)
    assert failure is not None, f"expected ({chain}, {protocol}) to be rejected"
    code, message = failure
    assert code == grpc.StatusCode.INVALID_ARGUMENT
    assert "unsupported (chain, protocol) pair" in message


@pytest.mark.parametrize(
    "chain, protocol",
    sorted(SUPPORTED_POOL_PAIRS),
)
def test_validator_accepts_every_supported_pool_pair(chain: str, protocol: str):
    """Every entry in ``SUPPORTED_POOL_PAIRS`` MUST be accepted by the
    validator. Locks the constant against silent table drift."""
    request = _make_request(chain=chain, protocol=protocol)
    assert _validate_pool_history_request(request, now_seconds=_NOW) is None


def test_supported_pool_pairs_set_includes_base_aerodrome():
    """Anti-regression: removing the Aerodrome row would silently
    eliminate test_chain_matrix_arbitrum_ethereum_base/base.aerodrome
    coverage. Pin the table contents here."""
    assert ("base", "aerodrome") in SUPPORTED_POOL_PAIRS
    # Aerodrome is NOT supported on any other chain in VIB-4728 scope.
    for chain in ("ethereum", "arbitrum", "optimism", "polygon"):
        assert (chain, "aerodrome") not in SUPPORTED_POOL_PAIRS


def test_pool_protocol_allowlist_locked():
    """Anti-regression: extending the allowlist requires a CODE change
    here, not a silent table edit. POOL-5 adds providers, not protocols."""
    assert POOL_PROTOCOL_ALLOWLIST == frozenset({"uniswap_v3", "aerodrome"})


# ============================================================================
# Inherited row #3 / #13: chain-aware normalize (strip + EVM lowercase)
# ============================================================================


def test_normalize_evm_strips_and_lowercases():
    """Inherited #3 + #13: EVM addresses are case-insensitive; the
    validator MUST normalize before hashing into a cache key. Two
    requests differing only in case / whitespace MUST produce the same
    canonical form."""
    canonical = normalize_pool_address("0xC6962004F452BE9203591991D15F6B388E09E8D0", "arbitrum")
    assert canonical == _VALID_EVM_POOL
    canonical_padded = normalize_pool_address(
        "  0xC6962004F452BE9203591991D15F6B388E09E8D0  ", "arbitrum"
    )
    assert canonical_padded == _VALID_EVM_POOL


def test_validator_accepts_mixed_case_evm_address():
    """A mixed-case EVM address with whitespace MUST be accepted (it's
    normalized first)."""
    request = _make_request(
        pool_address="  0xC6962004F452BE9203591991D15F6B388E09E8D0  ",
    )
    assert _validate_pool_history_request(request, now_seconds=_NOW) is None


def test_normalize_solana_preserves_case():
    """Inherited #3: Solana base58 is case-sensitive. ``.lower()`` would
    yield a different address. The normalize helper MUST only ``.strip()``."""
    raw = "  " + _VALID_SOLANA_POOL + "  "
    assert normalize_pool_address(raw, "solana") == _VALID_SOLANA_POOL
    # Lower-casing changes the address (proves case-sensitivity):
    lowered = _VALID_SOLANA_POOL.lower()
    assert lowered != _VALID_SOLANA_POOL


def test_validate_address_syntax_evm_regex():
    """Inherited #4: address syntax is verified BEFORE any URL/query
    interpolation. The regex rejects path-traversal and shell-meta
    characters because they cannot appear in ``[0-9a-f]{40}``."""
    assert validate_pool_address_syntax(_VALID_EVM_POOL, "arbitrum") is True
    assert validate_pool_address_syntax("0x" + "z" * 40, "arbitrum") is False
    # Length must be exactly 40 hex chars.
    assert validate_pool_address_syntax("0x" + "a" * 39, "arbitrum") is False
    assert validate_pool_address_syntax("0x" + "a" * 41, "arbitrum") is False
    # Path-traversal would fail the regex anchors / charset.
    assert validate_pool_address_syntax("0x../etc/passwd", "arbitrum") is False
    # No URL component allowed.
    assert validate_pool_address_syntax(
        "0xc6962004f452be9203591991d15f6b388e09e8d0?q=1", "arbitrum"
    ) is False


def test_validate_address_syntax_solana_regex():
    assert validate_pool_address_syntax(_VALID_SOLANA_POOL, "solana") is True
    # base58 alphabet excludes 0, O, I, l
    assert validate_pool_address_syntax("0" * 32, "solana") is False
    assert validate_pool_address_syntax("l" * 32, "solana") is False
    # Length bounds 32-44 chars
    assert validate_pool_address_syntax("a" * 31, "solana") is False
    assert validate_pool_address_syntax("a" * 45, "solana") is False


# ============================================================================
# Resolution + soft-cap helpers
# ============================================================================


@pytest.mark.parametrize(
    "resolution, expected_seconds",
    [
        (gateway_pb2.Resolution.RESOLUTION_1H, 3600),
        (gateway_pb2.Resolution.RESOLUTION_4H, 14400),
        (gateway_pb2.Resolution.RESOLUTION_1D, 86400),
    ],
)
def test_resolution_to_seconds(resolution: int, expected_seconds: int):
    assert resolution_to_seconds(resolution) == expected_seconds


def test_resolution_to_seconds_raises_on_unspecified():
    with pytest.raises(ValueError):
        resolution_to_seconds(gateway_pb2.Resolution.RESOLUTION_UNSPECIFIED)


@pytest.mark.parametrize(
    "resolution, expected_days",
    [
        (gateway_pb2.Resolution.RESOLUTION_1H, 90),
        (gateway_pb2.Resolution.RESOLUTION_4H, 180),
        (gateway_pb2.Resolution.RESOLUTION_1D, 730),
    ],
)
def test_soft_cap_defaults(monkeypatch, resolution: int, expected_days: int):
    """Defaults match the POOL-0 spike sizing. The env vars MUST be unset
    so the defaults apply."""
    for env in (
        "ALMANAK_GATEWAY_POOL_HISTORY_MAX_DAYS_1H",
        "ALMANAK_GATEWAY_POOL_HISTORY_MAX_DAYS_4H",
        "ALMANAK_GATEWAY_POOL_HISTORY_MAX_DAYS_1D",
    ):
        monkeypatch.delenv(env, raising=False)
    settings = GatewaySettings()
    assert get_soft_cap_seconds(settings, resolution) == expected_days * 86400


def test_soft_cap_env_override(monkeypatch):
    """Operators MUST be able to tighten the cap without a code change.
    The override flows through ``GatewaySettings`` so the read path stays
    typed (config-boundary rule)."""
    monkeypatch.setenv("ALMANAK_GATEWAY_POOL_HISTORY_MAX_DAYS_1H", "30")
    settings = GatewaySettings()
    assert (
        get_soft_cap_seconds(settings, gateway_pb2.Resolution.RESOLUTION_1H)
        == 30 * 86400
    )


@pytest.mark.parametrize("bad", ["", "   ", "not-a-number", "0", "-5"])
def test_soft_cap_invalid_env_falls_back_to_default(monkeypatch, bad: str):
    """A malformed env var must NOT surprise the operator with an
    INVALID_ARGUMENT — the settings validator coerces it to the default
    soft cap silently."""
    monkeypatch.setenv("ALMANAK_GATEWAY_POOL_HISTORY_MAX_DAYS_1H", bad)
    settings = GatewaySettings()
    assert (
        get_soft_cap_seconds(settings, gateway_pb2.Resolution.RESOLUTION_1H)
        == 90 * 86400
    )


def test_get_soft_cap_seconds_rejects_invalid_resolution():
    settings = GatewaySettings()
    with pytest.raises(ValueError):
        get_soft_cap_seconds(settings, gateway_pb2.Resolution.RESOLUTION_UNSPECIFIED)


# ============================================================================
# Chain table contracts
# ============================================================================


def test_supported_normalization_chains_includes_solana_and_evm():
    """``SUPPORTED_NORMALIZATION_CHAINS`` is the union of every chain
    the gateway might be asked to normalize FOR. It is intentionally
    broader than ``SUPPORTED_POOL_PAIRS`` — Solana is in normalization
    scope (for case-preservation) but NOT in pool-pair scope."""
    assert "solana" in SUPPORTED_NORMALIZATION_CHAINS
    for evm in ("ethereum", "arbitrum", "base", "optimism", "polygon"):
        assert evm in SUPPORTED_NORMALIZATION_CHAINS


def test_is_solana_chain():
    assert is_solana_chain("solana") is True
    assert is_solana_chain("ethereum") is False
    assert is_solana_chain("arbitrum") is False
    assert is_solana_chain("base") is False


def test_is_supported_pool_pair():
    assert is_supported_pool_pair("arbitrum", "uniswap_v3") is True
    assert is_supported_pool_pair("base", "aerodrome") is True
    assert is_supported_pool_pair("ethereum", "aerodrome") is False
    assert is_supported_pool_pair("solana", "uniswap_v3") is False


# ============================================================================
# Servicer integration: validator failures land on the gRPC handler
# ============================================================================


class _CodeContext:
    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _enabled_servicer() -> PoolHistoryServiceServicer:
    return PoolHistoryServiceServicer(GatewaySettings(pool_history_enabled=True))


def test_handler_returns_invalid_argument_for_empty_pool_address():
    """UAT card D3.F6 validator-level fast path: empty ``pool_address``
    MUST be rejected at the handler with INVALID_ARGUMENT BEFORE any
    provider is consulted. (Providers aren't yet wired in POOL-3, but
    the failure-envelope shape is what matters.)"""
    servicer = _enabled_servicer()
    ctx = _CodeContext()
    request = gateway_pb2.PoolHistoryRequest(
        pool_address="",
        chain="arbitrum",
        protocol="uniswap_v3",
        start_ts=_VALID_START,
        end_ts=_VALID_END,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )
    response = asyncio.run(servicer.GetPoolHistory(request, ctx))  # type: ignore[arg-type]
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert response.success is False
    # Failure envelope shape (UAT card D3.F6) — all metadata is non-stale.
    assert response.truncation_reason == gateway_pb2.TruncationReason.TRUNCATION_REASON_UNSPECIFIED
    assert response.next_start_ts == 0
    assert response.finalized_only is False
    assert response.source == ""
    assert len(response.snapshots) == 0
    assert "pool_address is required" in response.error


def test_handler_returns_invalid_argument_for_traversal_protocol():
    """UAT card D3.F6 cache-key-injection guard: a ``protocol`` value
    that looks like a path-traversal MUST be rejected at validation."""
    servicer = _enabled_servicer()
    ctx = _CodeContext()
    request = gateway_pb2.PoolHistoryRequest(
        pool_address=_VALID_EVM_POOL,
        chain="arbitrum",
        protocol="../etc/passwd",
        start_ts=_VALID_START,
        end_ts=_VALID_END,
        resolution=gateway_pb2.Resolution.RESOLUTION_1H,
    )
    response = asyncio.run(servicer.GetPoolHistory(request, ctx))  # type: ignore[arg-type]
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT
    assert response.success is False
    assert "unsupported protocol" in response.error


def test_handler_killswitch_off_does_NOT_run_validator():
    """Disabled deployment returns a uniform UNAVAILABLE message regardless
    of how malformed the request is — the validator is short-circuited by
    the kill-switch check (so operators get an actionable "enable the
    feature" message instead of a noise of validation errors)."""
    settings = GatewaySettings(pool_history_enabled=False)
    servicer = PoolHistoryServiceServicer(settings)
    ctx = _CodeContext()
    bad_request = gateway_pb2.PoolHistoryRequest()  # empty everything
    response = asyncio.run(servicer.GetPoolHistory(bad_request, ctx))  # type: ignore[arg-type]
    assert ctx.code == grpc.StatusCode.UNAVAILABLE  # NOT INVALID_ARGUMENT
    assert "VIB-4728" in response.error


def test_handler_valid_request_still_returns_unimplemented_in_pool3_window():
    """Validator passing does NOT mean providers are wired — POOL-3 is
    the validator ticket; POOL-5 (VIB-4753) wires providers. A
    syntactically-valid request still returns UNIMPLEMENTED until then."""
    servicer = _enabled_servicer()
    ctx = _CodeContext()
    response = asyncio.run(servicer.GetPoolHistory(_make_request(), ctx))  # type: ignore[arg-type]
    assert ctx.code == grpc.StatusCode.UNIMPLEMENTED
    assert response.success is False
    assert "POOL-5" in ctx.details or "VIB-4753" in ctx.details
