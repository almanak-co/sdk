"""VIB-4477 (T08): LP primitive_version bump — parallel V3 / V4 streams.

The Primitives Refactor Foundation (VIB-4160) introduced per-primitive
version isolation so a bump on one primitive cannot retroactively rebase
sibling primitives' historical scores. T08 extends that contract to the
V3 / V4 split inside the LP primitive family:

  * :attr:`Primitive.LP` continues to cover V3, Aerodrome, TraderJoe,
    Curve, PancakeSwap V3, SushiSwap V3, etc. (``primitive_version=1``,
    ``matching_policy_version=3``).
  * :attr:`Primitive.LP_V4` is a parallel slot for Uniswap V4 only
    (``primitive_version=1``, ``matching_policy_version=1``).

Resolution happens via :func:`primitive_for(event_type, protocol)`: when
the event_type's plain :func:`record_for` lookup returns :attr:`Primitive.LP`
AND the protocol contains ``"uniswap_v4"``, the override flips to
:attr:`Primitive.LP_V4`. Everything else (other LP venues, non-LP intents)
is unchanged.

Failure mode the regression test exists to prevent: someone bumps
``PRIMITIVE_VERSIONS[Primitive.LP]`` to 2 directly because "V4 ships a new
contract", silently re-baselining every V3 fixture and every historical
V3 LP row. The parallel slot makes that bump expressible without the
collateral damage — and the regression below catches the silent-migration
variant.
"""

from __future__ import annotations

import pytest

from almanak.framework.accounting.payload_schemas import (
    MATCHING_POLICY_VERSIONS,
    PRIMITIVE_VERSIONS,
)
from almanak.framework.accounting.policy import MatchingPolicy, PrimitiveVersion
from almanak.framework.primitives.taxonomy import primitive_for, record_for
from almanak.framework.primitives.types import Primitive


# =============================================================================
# 1. Enum + dict registration
# =============================================================================


class TestPrimitiveEnumRegistration:
    def test_lp_v4_is_a_primitive_member(self):
        assert Primitive.LP_V4.value == "lp_v4"
        assert Primitive.LP_V4 in set(Primitive)

    def test_lp_remains_a_distinct_primitive(self):
        assert Primitive.LP.value == "lp"
        assert Primitive.LP != Primitive.LP_V4

    def test_lp_v4_in_matching_policy_versions(self):
        assert Primitive.LP_V4 in MATCHING_POLICY_VERSIONS
        assert MATCHING_POLICY_VERSIONS[Primitive.LP_V4] == 1

    def test_lp_v4_in_primitive_versions(self):
        assert Primitive.LP_V4 in PRIMITIVE_VERSIONS
        assert PRIMITIVE_VERSIONS[Primitive.LP_V4] == 1

    def test_every_primitive_has_both_version_entries(self):
        """Writer lookup must never KeyError — every Primitive must be
        present in both dicts."""
        for primitive in Primitive:
            assert primitive in MATCHING_POLICY_VERSIONS, (
                f"Primitive.{primitive.name} missing from MATCHING_POLICY_VERSIONS"
            )
            assert primitive in PRIMITIVE_VERSIONS, (
                f"Primitive.{primitive.name} missing from PRIMITIVE_VERSIONS"
            )


# =============================================================================
# 2. Per-primitive version accessor isolation
# =============================================================================


class TestVersionAccessorIsolation:
    def test_lp_v3_stream_at_v1(self):
        assert PrimitiveVersion.for_primitive(Primitive.LP) == 1

    def test_lp_v4_stream_at_v1(self):
        assert PrimitiveVersion.for_primitive(Primitive.LP_V4) == 1

    def test_matching_policy_v3_at_3(self):
        assert MatchingPolicy.for_primitive(Primitive.LP) == 3

    def test_matching_policy_v4_at_1(self):
        """V4's lot-matching version starts fresh at 1 — V0 contract."""
        assert MatchingPolicy.for_primitive(Primitive.LP_V4) == 1


# =============================================================================
# 3. primitive_for(...) protocol-aware override
# =============================================================================


class TestPrimitiveForOverride:
    def test_lp_open_with_uniswap_v4_resolves_to_lp_v4(self):
        assert primitive_for("LP_OPEN", "uniswap_v4") is Primitive.LP_V4

    def test_lp_close_with_uniswap_v4_resolves_to_lp_v4(self):
        assert primitive_for("LP_CLOSE", "uniswap_v4") is Primitive.LP_V4

    def test_lp_collect_fees_with_uniswap_v4_resolves_to_lp_v4(self):
        assert primitive_for("LP_COLLECT_FEES", "uniswap_v4") is Primitive.LP_V4

    @pytest.mark.parametrize(
        "protocol",
        ["uniswap_v3", "aerodrome", "aerodrome_slipstream", "traderjoe_v2", "curve", "pancakeswap_v3", "sushiswap_v3"],
    )
    def test_other_lp_venues_stay_at_primitive_lp(self, protocol: str):
        """V0 of the V4 split: only ``uniswap_v4`` overrides; other LP
        venues continue to resolve to ``Primitive.LP``."""
        assert primitive_for("LP_OPEN", protocol) is Primitive.LP
        assert primitive_for("LP_CLOSE", protocol) is Primitive.LP

    def test_empty_protocol_falls_back_to_record_for(self):
        """No protocol context = no override; LP events resolve to
        ``Primitive.LP``."""
        assert primitive_for("LP_OPEN", "") is Primitive.LP

    def test_non_lp_intent_with_uniswap_v4_protocol_unaffected(self):
        """Override only flips LP -> LP_V4; non-LP intents stay on their
        canonical primitive even when protocol is uniswap_v4."""
        # SWAP via uniswap_v4 is still Primitive.SWAP — version splits per
        # primitive, not per protocol-tagged event.
        assert primitive_for("SWAP", "uniswap_v4") is Primitive.SWAP

    def test_unknown_intent_type_falls_back_to_utility(self):
        """Unknown event_types collapse to Primitive.UTILITY rather than
        raising — matches the writer chokepoint's non-live fallback."""
        assert primitive_for("DEFINITELY_NOT_A_REAL_INTENT", "uniswap_v4") is Primitive.UTILITY


# =============================================================================
# 4. record_for(...) stays V3/V4-agnostic for routing
# =============================================================================


class TestRecordForUnchanged:
    """``record_for`` consumers (the AccountingCategory dispatcher) do NOT
    distinguish V3 from V4 — both route through ``lp_handler``. The
    primitive-version split is purely a version-stamping concern, so
    ``record_for`` must keep returning ``Primitive.LP`` for every LP event_type
    regardless of protocol."""

    def test_record_for_lp_open_returns_primitive_lp(self):
        assert record_for("LP_OPEN").primitive is Primitive.LP

    def test_record_for_lp_close_returns_primitive_lp(self):
        assert record_for("LP_CLOSE").primitive is Primitive.LP

    def test_record_for_does_not_know_about_lp_v4(self):
        """record_for has no protocol argument — it cannot distinguish
        V3 from V4 and is intentionally V-version-agnostic."""
        # Calling primitive_for without protocol should match record_for.
        for event_type in ("LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"):
            assert primitive_for(event_type, "") is record_for(event_type).primitive


# =============================================================================
# 5. No silent migration — bumping LP_V4 must not affect LP and vice-versa
# =============================================================================


class TestNoSilentMigration:
    def test_bumping_lp_v4_does_not_affect_lp(self, monkeypatch: pytest.MonkeyPatch):
        """Mutating PRIMITIVE_VERSIONS[Primitive.LP_V4] = 99 must NOT change
        the version returned for Primitive.LP."""
        monkeypatch.setitem(PRIMITIVE_VERSIONS, Primitive.LP_V4, 99)
        assert PrimitiveVersion.for_primitive(Primitive.LP_V4) == 99
        # V3 stream is untouched.
        assert PrimitiveVersion.for_primitive(Primitive.LP) == 1

    def test_bumping_lp_does_not_affect_lp_v4(self, monkeypatch: pytest.MonkeyPatch):
        """And vice-versa — V3 bumps must not retro-baseline V4."""
        monkeypatch.setitem(PRIMITIVE_VERSIONS, Primitive.LP, 99)
        assert PrimitiveVersion.for_primitive(Primitive.LP) == 99
        assert PrimitiveVersion.for_primitive(Primitive.LP_V4) == 1

    def test_bumping_lp_v4_matching_policy_does_not_affect_lp(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """matching_policy_version isolation mirrors primitive_version
        isolation — VIB-4162 contract preserved across the V3/V4 split."""
        monkeypatch.setitem(MATCHING_POLICY_VERSIONS, Primitive.LP_V4, 7)
        assert MatchingPolicy.for_primitive(Primitive.LP_V4) == 7
        assert MatchingPolicy.for_primitive(Primitive.LP) == 3


# =============================================================================
# 6. Writer chokepoint stamps the V4 versions on uniswap_v4 payloads
# =============================================================================


class TestWriterChokepointProtocolAware:
    """``augment_accounting_payload`` is the single stamping site; it MUST
    apply the LP -> LP_V4 override when the payload's ``protocol`` is
    ``uniswap_v4`` and pull versions from the LP_V4 slot."""

    def _augment(self, *, protocol: str, monkeypatch: pytest.MonkeyPatch | None = None) -> dict:
        import json as _json

        from almanak.framework.accounting.writer import augment_accounting_payload

        payload = {
            "event_type": "LP_OPEN",
            "protocol": protocol,
            "position_key": "lp:x:arbitrum:0xwallet:0xpool",
            "pool_address": "0x" + "ab" * 32 if protocol == "uniswap_v4" else "0x" + "ab" * 20,
            "token0": "WETH",
            "token1": "USDC",
            "amount0": "1",
            "amount1": "2000",
            "confidence": "HIGH",
        }
        out = augment_accounting_payload(_json.dumps(payload), is_live=False)
        return _json.loads(out)

    def test_v4_payload_stamps_lp_v4_version_slots(self):
        d = self._augment(protocol="uniswap_v4")
        assert d["primitive_version"] == PRIMITIVE_VERSIONS[Primitive.LP_V4]
        assert d["matching_policy_version"] == MATCHING_POLICY_VERSIONS[Primitive.LP_V4]

    def test_v3_payload_stamps_lp_version_slots(self):
        d = self._augment(protocol="uniswap_v3")
        assert d["primitive_version"] == PRIMITIVE_VERSIONS[Primitive.LP]
        assert d["matching_policy_version"] == MATCHING_POLICY_VERSIONS[Primitive.LP]

    def test_v4_bump_does_not_leak_into_v3_stamp(self, monkeypatch: pytest.MonkeyPatch):
        """Critical isolation: bumping V4's version must NOT change the
        stamp on V3 payloads going through the same chokepoint."""
        monkeypatch.setitem(PRIMITIVE_VERSIONS, Primitive.LP_V4, 9)
        v4 = self._augment(protocol="uniswap_v4")
        v3 = self._augment(protocol="uniswap_v3")
        assert v4["primitive_version"] == 9
        assert v3["primitive_version"] == 1  # untouched by the V4 bump
